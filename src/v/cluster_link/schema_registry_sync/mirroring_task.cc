/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/schema_registry_sync/mirroring_task.h"

#include "cluster_link/link.h"
#include "cluster_link/schema_registry_sync/reconciler.h"
#include "cluster_link/schema_registry_sync/scope.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/defer.hh>

#include <utility>

namespace cluster_link::schema_registry_sync {

namespace {

ss::lowres_clock::duration
tail_interval(const model::schema_registry_sync_config& cfg) {
    if (const auto* api = cfg.api_mode(); api != nullptr) {
        return api->get_tail_interval();
    }
    return model::schema_registry_sync_config::shadow_schema_registry_api::
      default_tail_interval;
}

ss::lowres_clock::duration
full_sync_interval(const model::schema_registry_sync_config& cfg) {
    if (const auto* api = cfg.api_mode(); api != nullptr) {
        return api->get_full_sync_interval();
    }
    return model::schema_registry_sync_config::shadow_schema_registry_api::
      default_full_sync_interval;
}

} // namespace

ss::future<inventory> scan_destination_inventory(
  schema::registry& destination,
  ss::noncopyable_function<bool(const ppsr::context_subject&)> in_scope,
  ss::abort_source& as) {
    as.check();
    auto versions = co_await destination.list_subject_versions(
      std::move(in_scope), ppsr::include_deleted::yes);
    inventory inv;
    inv.all.reserve(versions.size());
    for (const auto& sv : versions) {
        auto node = ppsr::subject_version{sv.sub, sv.version};
        if (sv.deleted == ppsr::is_deleted::no) {
            inv.active.insert(node);
        }
        inv.all.insert(std::move(node));
    }
    co_return inv;
}

mirroring_task::mirroring_task(
  link* link,
  const model::metadata& link_metadata,
  schema::registry* destination,
  source_reader_factory* source_factory)
  : task(
      link,
      tail_interval(link_metadata.configuration.schema_registry_sync_cfg),
      mirroring_task::task_name)
  , _config(link_metadata.configuration.schema_registry_sync_cfg.copy())
  , _destination(destination)
  , _source_factory(source_factory)
  , _reader(_source_factory->create()) {}

void mirroring_task::update_config(const model::metadata& link_metadata) {
    _config = link_metadata.configuration.schema_registry_sync_cfg.copy();
    set_run_interval(tail_interval(_config));
    // The scope (filters/contexts) may have changed; flag a forced full scan so
    // the next run re-derives the inventory. Only a flag is set here: mutating
    // _status/_last_full_sync would race an in-flight run_impl that resumes and
    // overwrites it.
    _config_changed = true;
}

model::enabled_t mirroring_task::is_enabled() const {
    const auto* api = _config.api_mode();
    return model::enabled_t(api != nullptr && bool(api->is_enabled));
}

bool mirroring_task::leads_schema_registry_partition() const {
    return get_link()->partition_manager().is_current_shard_leader(
      ::model::schema_registry_internal_ntp);
}

bool mirroring_task::should_start_impl(ss::shard_id, ::model::node_id) const {
    return leads_schema_registry_partition();
}

bool mirroring_task::should_stop_impl(ss::shard_id, ::model::node_id) const {
    return !leads_schema_registry_partition();
}

bool mirroring_task::should_long_sync() const {
    if (!_last_full_sync.has_value()) {
        return true;
    }
    return ss::lowres_clock::now() - *_last_full_sync
           >= full_sync_interval(_config);
}

model::task_status_report mirroring_task::get_status_report() const {
    auto report = task::get_status_report();
    // Only the shard leading _schemas/0 runs the sync; a stopped shard's empty
    // status must not win the admin aggregation over the leader's, so suppress
    // it.
    if (get_state() != model::task_state::stopped) {
        auto status = _status;
        // Reflect the in-flight reconcile's live counters for mid-sync
        // progress. Guarded on current_sync so it cannot double-count after the
        // fold (which zeroes _reconcile_stats and bakes them into _status).
        if (status.current_sync.has_value()) {
            status.current_sync->summary.subject_versions_changed
              += _reconcile_stats.versions_changed;
            status.current_sync->summary.errors += _reconcile_stats.errors;
            status.totals_since_task_start.subject_versions_changed
              += _reconcile_stats.versions_changed;
            status.totals_since_task_start.errors += _reconcile_stats.errors;
        }
        report.detail = model::task_detail{
          .schema_registry_sync_status = std::move(status)};
    }
    return report;
}

ss::future<> mirroring_task::refresh_destination_inventory(
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>& in_scope,
  ss::abort_source& as) {
    // Destination/internal faults bubble out and become `faulted`. The scan
    // sinks the predicate by value; forward the borrowed one.
    _destination_inventory = co_await scan_destination_inventory(
      *_destination,
      [&in_scope](const ppsr::context_subject& cs) { return in_scope(cs); },
      as);

    chunked_hash_set<ppsr::context_subject> subjects;
    for (const auto& key : _destination_inventory.active) {
        subjects.insert(key.sub);
    }
    _status.inventory.destination_subjects = static_cast<uint64_t>(
      subjects.size());
    _status.inventory.destination_subject_versions = static_cast<uint64_t>(
      _destination_inventory.active.size());
}

void mirroring_task::record_error(std::string_view what) {
    ++_status.current_sync->summary.errors;
    ++_status.totals_since_task_start.errors;
    _status.last_error_message = ss::sstring{what};
    vlog(logger().warn, "Schema Registry sync error: {}", what);
}

ss::future<std::optional<chunked_vector<ppsr::schema_version>>>
mirroring_task::list_versions_once(
  const ppsr::context_subject& subject,
  ppsr::include_deleted include_deleted,
  ss::abort_source& as,
  std::optional<source_error>& unavailable) {
    as.check();
    auto res = co_await _reader->list_subject_versions(
      subject, include_deleted, as);
    if (res.has_value()) {
        co_return std::move(res.value());
    }
    if (res.error().kind == source_error_kind::source_unavailable) {
        if (!unavailable.has_value()) {
            unavailable = std::move(res.error());
        }
        co_return std::nullopt;
    }
    // Reachable but failed (rare delete race): count and skip. Counters are
    // touched only between co_awaits, so sharing them across the concurrent
    // fibers is safe on one reactor.
    record_error(res.error().message);
    co_return std::nullopt;
}

ss::future<> mirroring_task::list_one_subject(
  const ppsr::context_subject& subject,
  ss::abort_source& as,
  chunked_hash_set<ppsr::subject_version>& source_active,
  chunked_hash_set<ppsr::subject_version>& source_deleted,
  std::optional<source_error>& unavailable) {
    // A peer fiber already hit source_unavailable; skip the remaining work.
    if (unavailable.has_value()) {
        co_return;
    }
    // Two listings recover the per-version deleted state the bare source
    // listing omits: `active` are the non-deleted versions; the remainder of
    // `all` are soft-deleted. Short-circuit on the first failure so a failing
    // subject counts at most one error.
    auto active = co_await list_versions_once(
      subject, ppsr::include_deleted::no, as, unavailable);
    if (!active.has_value()) {
        co_return;
    }
    auto all = co_await list_versions_once(
      subject, ppsr::include_deleted::yes, as, unavailable);
    if (!all.has_value()) {
        co_return;
    }
    chunked_hash_set<ppsr::schema_version> active_set;
    for (auto version : *active) {
        active_set.insert(version);
        source_active.insert(ppsr::subject_version{subject, version});
    }
    for (auto version : *all) {
        if (!active_set.contains(version)) {
            source_deleted.insert(ppsr::subject_version{subject, version});
        }
    }
}

ss::future<task::state_transition> mirroring_task::full_source_sync(
  ss::abort_source& as,
  const chunked_hash_set<ppsr::context>& contexts,
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
    in_scope) {
    // Cluster-global, so safe to read mid-sync (unlike the per-link config a
    // concurrent update_config can swap). The one parallelism bound governs
    // both the version-listing fan-out and the reconcile's import concurrency.
    auto limits = reconciler::limits{
      .memory_bytes
      = config::shard_local_cfg().schema_registry_sync_memory_bytes(),
      .parallelism
      = config::shard_local_cfg().schema_registry_sync_parallelism()};

    // Discover the source nodes; the reconciler fetches the bodies. Active and
    // soft-deleted are tracked apart so the work-set diff can treat them
    // differently (see below).
    chunked_hash_set<ppsr::subject_version> source_active;
    chunked_hash_set<ppsr::subject_version> source_deleted;

    // Contexts are few, so enumerate their subjects sequentially. in_scope also
    // scopes discovery, keeping source and destination sides consistent.
    chunked_vector<ppsr::context_subject> subjects;
    for (const auto& ctx : contexts) {
        auto subjects_res = co_await _reader->list_subjects(ctx, as);
        if (!subjects_res.has_value()) {
            if (
              subjects_res.error().kind
              == source_error_kind::source_unavailable) {
                co_return make_unavailable(subjects_res.error().message);
            }
            // Reachable but failed (rare delete race): count and skip.
            record_error(subjects_res.error().message);
            continue;
        }
        for (auto& subject : subjects_res.value()) {
            if (in_scope(subject)) {
                subjects.push_back(std::move(subject));
            }
        }
    }
    _status.inventory.selected_source_subjects = subjects.size();

    // Bounded concurrency over independent round-trips. list_one_subject is a
    // member, not a coroutine lambda (see its declaration), so the forwarding
    // lambda only returns its future.
    std::optional<source_error> unavailable;
    co_await ss::max_concurrent_for_each(
      subjects,
      std::max<size_t>(1, limits.parallelism),
      [&](const ppsr::context_subject& subject) {
          return list_one_subject(
            subject, as, source_active, source_deleted, unavailable);
      });
    if (unavailable.has_value()) {
        co_return make_unavailable(unavailable->message);
    }

    // Every discovered source version is selected for sync, soft-deleted ones
    // included (they are imported below), so count both -- mirroring
    // selected_source_subjects, a discovery count rather than a change count.
    _status.inventory.selected_source_subject_versions = static_cast<uint64_t>(
      source_active.size() + source_deleted.size());

    // State-aware. An active source version missing from the destination's
    // active set is imported (creating it, or reactivating one that is
    // soft-deleted on the destination -- the seed does not suppress work
    // items). A soft-deleted source version is imported unless it is already
    // soft-deleted on the destination: an absent version is imported
    // soft-deleted, and one still active on the destination has its deleted
    // body re-imported to propagate the soft-delete (import overwrites the
    // version's deleted flag). Soft-delete propagation covers source-present
    // versions in both directions; purging destination-only versions
    // (hard-delete propagation) is deferred, as is detecting divergent
    // same-key content (a matching key is assumed to mean matching content --
    // the destination is a managed mirror).
    work_set work;
    for (const auto& node : source_active) {
        if (!_destination_inventory.active.contains(node)) {
            work.upserts.push_back(node);
        }
    }
    for (const auto& node : source_deleted) {
        const bool dest_deleted = _destination_inventory.all.contains(node)
                                  && !_destination_inventory.active.contains(
                                    node);
        if (!dest_deleted) {
            work.upserts.push_back(node);
        }
    }

    // The reconciler sinks the predicate by value; forward the borrowed one.
    auto rec = reconciler{
      _reader.get(),
      _destination,
      [&in_scope](const ppsr::context_subject& cs) { return in_scope(cs); },
      limits};

    // The reconciler increments _reconcile_stats live (reflected mid-sync by
    // get_status_report); the fold below moves them into persistent state.
    _reconcile_stats = reconcile_stats{};
    // Seed with the full (active + soft-deleted) set: soft-deleted nodes still
    // satisfy references. reconcile sinks it by value and
    // _destination_inventory is rebuilt next run, so move `all` in rather than
    // copy it.
    auto result = co_await rec.reconcile(
      std::move(work),
      std::move(_destination_inventory.all),
      _reconcile_stats,
      as);
    if (!result.has_value()) {
        if (result.error().kind == source_error_kind::source_unavailable) {
            co_return make_unavailable(result.error().message);
        }
        // reconcile only surfaces source_unavailable today; treat any other
        // error defensively as a counted per-item failure.
        record_error(result.error().message);
        co_return make_active();
    }

    const auto stats = _reconcile_stats;
    // Fold once into persistent state, then clear so the report-time reflection
    // cannot double-count.
    _reconcile_stats = reconcile_stats{};
    _status.current_sync->summary.subject_versions_changed
      += stats.versions_changed;
    _status.current_sync->summary.errors += stats.errors;
    _status.totals_since_task_start.subject_versions_changed
      += stats.versions_changed;
    _status.totals_since_task_start.errors += stats.errors;

    // Re-scan now that imports have landed so the reported destination counts
    // reflect the post-sync state, not the pre-import baseline the diff used.
    co_await refresh_destination_inventory(in_scope, as);

    vlog(
      logger().info,
      "Schema Registry full sync: {} source subjects ({} versions), {} "
      "destination subjects; imported {} versions, {} errors",
      _status.inventory.selected_source_subjects,
      _status.inventory.selected_source_subject_versions,
      _status.inventory.destination_subjects,
      stats.versions_changed,
      stats.errors);

    _status.current_sync->summary.finish_time = ::model::timestamp::now();
    _status.last_full_sync = _status.current_sync->summary;
    // Completed (best-effort, per-item failures counted), so advance the timer
    // and retry on the normal interval.
    _last_full_sync = ss::lowres_clock::now();
    co_return make_active();
}

ss::future<task::state_transition>
mirroring_task::run_impl(ss::abort_source& as) {
    // Consume the config-changed flag before any co_await so a concurrent
    // update_config during this run is not lost (it re-arms for the next run).
    const bool long_sync = std::exchange(_config_changed, false)
                           || should_long_sync();

    _status.current_sync = model::schema_registry_current_sync{
      .sync_type = long_sync ? model::schema_registry_sync_type::full
                             : model::schema_registry_sync_type::tail,
      .summary = {.start_time = ::model::timestamp::now()}};
    // current_sync reflects an in-progress sync only; clear it on every exit
    // (success, unavailable, or a fault that throws out of run_impl) so a stale
    // partial summary is never reported between runs.
    auto clear_current_sync = ss::defer(
      [this] { _status.current_sync.reset(); });

    if (!long_sync) {
        // Incremental tail sync is not implemented yet; nothing to do on a
        // tail tick (in particular, do not rescan the destination).
        vlog(logger().debug, "Schema Registry tail sync not yet implemented");
        co_return make_active();
    }

    // Contexts to replicate = source contexts intersected with the filter.
    auto contexts_res = co_await _reader->list_contexts(as);
    if (!contexts_res.has_value()) {
        if (
          contexts_res.error().kind == source_error_kind::source_unavailable) {
            co_return make_unavailable(contexts_res.error().message);
        }
        record_error(contexts_res.error().message);
        co_return make_active();
    }
    // Filter has union semantics: filter.contexts selects whole contexts,
    // filter.subjects adds individual qualified subjects (each carrying its own
    // context), and an empty filter replicates everything.
    const auto qualified = ppsr::qualified_subjects_enabled{
      config::shard_local_cfg().schema_registry_enable_qualified_subjects()};
    chunked_hash_set<ppsr::context> filter_contexts;
    chunked_hash_set<ppsr::context_subject> filter_subjects;
    const auto* api = _config.api_mode();
    if (api == nullptr) {
        vlog(
          logger().debug,
          "Schema Registry sync disabled mid-run; skipping remainder of sync");
        co_return make_active();
    }
    for (const auto& ctx : api->filter.contexts) {
        filter_contexts.insert(ppsr::context{ctx});
    }
    for (const auto& sub : api->filter.subjects) {
        filter_subjects.insert(
          ppsr::context_subject::from_string(sub, qualified));
    }
    const bool unfiltered = filter_contexts.empty() && filter_subjects.empty();
    // A filtered subject's context must be scanned even when the context filter
    // omits it, else that subject is never discovered.
    chunked_hash_set<ppsr::context> subject_contexts;
    for (const auto& cs : filter_subjects) {
        subject_contexts.insert(cs.ctx);
    }
    chunked_hash_set<ppsr::context> contexts;
    for (auto& ctx : contexts_res.value()) {
        if (
          unfiltered || filter_contexts.contains(ctx)
          || subject_contexts.contains(ctx)) {
            contexts.insert(std::move(ctx));
        }
    }

    if (
      auto reason = check_preconditions(
        _config,
        contexts,
        config::shard_local_cfg().schema_registry_enable_qualified_subjects());
      reason.has_value()) {
        co_return make_faulted(*reason);
    }

    // in_scope is move-only (its filter sets are not copyable) but two by-value
    // sinks consume it per run (the destination scan and the reconciler), so
    // build it once and lend it by reference; each sink forwards a thin
    // wrapper.
    auto in_scope = make_in_scope(
      std::move(filter_contexts), std::move(filter_subjects));

    co_await refresh_destination_inventory(in_scope, as);
    co_return co_await full_source_sync(as, contexts, in_scope);
}

task::state_transition
mirroring_task::make_unavailable(const ss::sstring& reason) {
    vlog(
      logger().warn, "Schema Registry shadowing task unavailable: {}", reason);
    _status.last_error_message = reason;
    // No special backoff: an unavailable run leaves _last_full_sync unadvanced,
    // so the next tail tick re-attempts the full sync and the link recovers on
    // the normal cadence once the source comes back.
    return state_transition{
      .desired_state = model::task_state::link_unavailable, .reason = reason};
}

task::state_transition mirroring_task::make_active() {
    return state_transition{
      .desired_state = model::task_state::active,
      .reason = "Schema Registry shadowing task finished a sync"};
}

task::state_transition mirroring_task::make_faulted(const ss::sstring& reason) {
    vlog(logger().warn, "Schema Registry shadowing task faulted: {}", reason);
    _status.last_error_message = reason;
    return state_transition{
      .desired_state = model::task_state::faulted, .reason = reason};
}

std::string_view mirroring_task_factory::created_task_name() const noexcept {
    return mirroring_task::task_name;
}

std::unique_ptr<task> mirroring_task_factory::create_task(link* link) {
    return std::make_unique<mirroring_task>(
      link, *(link->get_config()), _destination, _source_factory);
}

} // namespace cluster_link::schema_registry_sync
