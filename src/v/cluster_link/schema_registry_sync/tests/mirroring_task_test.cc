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
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "cluster_link/schema_registry_sync/tests/sr_sync_test_fixtures.h"
#include "cluster_link/tests/deps.h"
#include "container/chunked_vector.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/tests/fake_registry.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <gmock/gmock.h>

using namespace std::chrono_literals;

namespace cluster_link::tests {

namespace {

static const model::name_t link_name{"test_sr_link"};
constexpr auto tail_interval = 1s;
constexpr auto wait_interval = 5s;

model::metadata get_default_metadata() {
    model::metadata metadata{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::
        connection_config{.bootstrap_servers = {net::unresolved_address("localhost", 9092)}},
      .state = model::link_state{}};

    model::schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "https://schema-registry.example.com";
    api.tail_interval = tail_interval;
    // Long enough that a second full sync within a test only happens because a
    // config change forced it, never on the periodic schedule.
    api.full_sync_interval = 1h;
    metadata.configuration.schema_registry_sync_cfg.sync_mode = std::move(api);
    return metadata;
}

} // namespace

class mirroring_task_test : public seastar_test {
public:
    static constexpr auto task_reconciler_interval = 1s;

    ss::future<> SetUpAsync() override {
        _clmtf = std::make_unique<cluster_link_manager_test_fixture>(self());
        co_await _clmtf->wire_up_and_start(
          std::make_unique<test_link_factory>(task_reconciler_interval));

        co_await _clmtf->get_manager().invoke_on_all([this](manager& m) {
            return m.register_task_factory<srs::mirroring_task_factory>(
              &_registry, &_source_factory);
        });

        fixture()->elect_leader(::model::controller_ntp, self(), std::nullopt);
    }

    ss::future<> TearDownAsync() override {
        co_await _clmtf->reset();
        _clmtf.reset();
    }

    cluster_link_manager_test_fixture* fixture() { return _clmtf.get(); }

    ::model::node_id self() { return ::model::node_id(0); }

    void lead_schema_registry() {
        fixture()->elect_leader(
          ::model::schema_registry_internal_ntp, self(), ss::this_shard_id());
    }

    void unlead_schema_registry() {
        fixture()->elect_leader(
          ::model::schema_registry_internal_ntp,
          ::model::node_id(1),
          std::nullopt);
    }

    // Seeds the destination registry with one (subject, version).
    void seed_destination(std::string_view subject, int32_t version) {
        _registry
          .import_schema(make_schema(
            ppsr::context_subject::unqualified(subject),
            version,
            fmt::format("{{\"v\":{}}}", version)))
          .get();
    }

    ss::future<bool> wait_for_task_state(model::task_state state) {
        return fixture()->wait_for_report_to_match(
          wait_interval,
          50ms,
          [state](const model::cluster_link_task_status_report& report) {
              const auto* sr = find_sr_status(report);
              return sr != nullptr && sr->task_state == state;
          });
    }

    static const model::task_status_report*
    find_sr_status(const model::cluster_link_task_status_report& report) {
        auto link_it = report.link_reports.find(link_name);
        if (link_it == report.link_reports.end()) {
            return nullptr;
        }
        auto task_it = link_it->second.task_status_reports.find(
          srs::mirroring_task::task_name);
        if (task_it == link_it->second.task_status_reports.end()) {
            return nullptr;
        }
        return &task_it->second;
    }

    // Extracts the Schema Registry status from a task report's detail.
    static const model::schema_registry_sync_status*
    sr_status(const model::task_status_report* report) {
        if (
          report == nullptr || !report->detail.has_value()
          || !report->detail->schema_registry_sync_status.has_value()) {
            return nullptr;
        }
        return &report->detail->schema_registry_sync_status.value();
    }

    ss::future<std::optional<model::schema_registry_sync_status>>
    wait_for_sync_status(
      std::function<bool(const model::schema_registry_sync_status&)> pred) {
        std::optional<model::schema_registry_sync_status> result;
        co_await ::tests::cooperative_spin_wait_with_timeout(
          wait_interval, [this, &pred, &result]() {
              auto report
                = fixture()->get_manager().local().get_task_status_report();
              const auto* sr = sr_status(find_sr_status(report));
              if (sr != nullptr && pred(*sr)) {
                  result = *sr;
                  return true;
              }
              return false;
          });
        co_return result;
    }

    fake_source_state _source_state;
    fake_source_reader_factory _source_factory{&_source_state};
    schema::fake_registry _registry;
    std::unique_ptr<cluster_link_manager_test_fixture> _clmtf;
};

TEST_F(mirroring_task_test, populates_source_and_destination_inventory) {
    auto subject = ppsr::context_subject::unqualified("orders-value");
    _source_state.add(subject, 1);
    _source_state.add(subject, 2);
    seed_destination("payments-value", 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value();
                  }).get();

    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->inventory.selected_source_subjects, 1);
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 2);
    // The destination counters are refreshed after the import, so they reflect
    // the post-sync state: the seeded payments-value plus the two imported
    // orders-value versions.
    EXPECT_EQ(status->inventory.destination_subjects, 2);
    EXPECT_EQ(status->inventory.destination_subject_versions, 3);
    // The two source versions are absent from the destination, so the reconcile
    // imports both.
    EXPECT_EQ(status->totals_since_task_start.subject_versions_changed, 2);
    EXPECT_EQ(status->last_full_sync->errors, 0);
}

TEST_F(mirroring_task_test, full_sync_imports_and_reports) {
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    auto c = ppsr::context_subject::unqualified("c");
    // a:v1 (no refs), b:v1 refs a:v1 (a small ref graph), c:v1 (no refs). The
    // engine must import a before b regardless of listing order.
    _source_state.add(a, 1);
    _source_state.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));
    _source_state.add(c, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && s.last_full_sync->subject_versions_changed == 3
                             && !s.current_sync.has_value();
                  }).get();

    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 3);
    EXPECT_EQ(status->last_full_sync->errors, 0);
    EXPECT_EQ(status->totals_since_task_start.subject_versions_changed, 3);
    // The cumulative summary's start time is stamped once on the task's first
    // run; it was previously left unset (only current_sync carried a start).
    EXPECT_TRUE(status->totals_since_task_start.start_time.has_value());

    // Create-only replication imports schema versions but never touches
    // compatibility configs, subject modes, or unsupported-feature handling,
    // so those deferred counters stay zero.
    EXPECT_EQ(status->last_full_sync->compatibility_configs_changed, 0);
    EXPECT_EQ(status->last_full_sync->modes_changed, 0);
    EXPECT_EQ(status->last_full_sync->unsupported_features_removed, 0);

    // Three source subjects (a, b, c) with one version each; the destination
    // was empty before the sync and, after the post-import refresh, mirrors all
    // three.
    EXPECT_EQ(status->inventory.selected_source_subjects, 3);
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 3);
    EXPECT_EQ(status->inventory.destination_subjects, 3);
    EXPECT_EQ(status->inventory.destination_subject_versions, 3);

    // The sync has finished: current_sync is cleared, and last_full_sync
    // carries both a start and a finish timestamp (start <= finish).
    EXPECT_FALSE(status->current_sync.has_value());
    ASSERT_TRUE(status->last_full_sync->start_time.has_value());
    ASSERT_TRUE(status->last_full_sync->finish_time.has_value());
    EXPECT_LE(
      status->last_full_sync->start_time->value(),
      status->last_full_sync->finish_time->value());

    // All three source versions landed on the destination, referent-first.
    const auto& all = _registry.get_all();
    EXPECT_EQ(all.size(), 3);
    EXPECT_LT(index_of(all, "a"), index_of(all, "b"));
}

TEST_F(mirroring_task_test, source_unavailable_then_recovers) {
    _source_state.add(ppsr::context_subject::unqualified("orders-value"), 1);
    _source_state.list_subjects_error = srs::source_error{
      .kind = srs::source_error_kind::source_unavailable,
      .message = "source down"};

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    // An unreachable source parks the task as link_unavailable.
    ASSERT_TRUE(wait_for_task_state(model::task_state::link_unavailable).get());

    // Once the source recovers, the next tick re-attempts the still-due full
    // sync (the unavailable run left the timer unadvanced) and reaches active.
    _source_state.list_subjects_error.reset();
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());
    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && s.inventory.selected_source_subjects == 1;
                  }).get();
    ASSERT_TRUE(status.has_value());
}

TEST_F(mirroring_task_test, config_update_forces_full_resync) {
    auto subject = ppsr::context_subject::unqualified("orders-value");
    _source_state.add(subject, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    // Wait for the first full sync. The default full-sync interval is long, so
    // without a config-triggered re-scan the inventory would not update again
    // soon.
    auto first = wait_for_sync_status([](const auto& s) {
                     return s.last_full_sync.has_value()
                            && s.inventory.selected_source_subjects == 1;
                 }).get();
    ASSERT_TRUE(first.has_value());

    // Change the source, then update the link config. The config change forces
    // a fresh full scan, so the new source is reflected promptly rather than
    // after the (1h) full-sync interval.
    _source_state.add(ppsr::context_subject::unqualified("payments-value"), 1);
    fixture()->update_link(model::id_t{0}, get_default_metadata()).get();

    auto second = wait_for_sync_status([](const auto& s) {
                      return s.inventory.selected_source_subjects == 2;
                  }).get();
    ASSERT_TRUE(second.has_value());
}

TEST_F(mirroring_task_test, source_list_failure_completes_and_advances) {
    auto ok = ppsr::context_subject::unqualified("orders-value");
    auto failing = ppsr::context_subject::unqualified("payments-value");
    _source_state.add(ok, 1);
    _source_state.add(failing, 1);
    // Listing payments-value's versions fails (reachable, not unavailable).
    // This is a rare source-side delete race: it is counted as a per-item error
    // and skipped, but the full sync still completes and advances the timer.
    _source_state.list_versions_errors.emplace(
      failing,
      srs::source_error{
        .kind = srs::source_error_kind::operation_failed,
        .message = "version listing failed"});

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    // The full sync completes best-effort: the error is counted, orders-value's
    // single version is listed and imported, and last_full_sync is recorded
    // (the timer advanced -- the failure does not force a fast retry).
    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_GE(status->totals_since_task_start.errors, 1);
    EXPECT_EQ(status->last_full_sync->errors, 1);
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 1);
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);

    // The reachable subject was still imported; the failing one was skipped.
    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1);
    EXPECT_EQ(all[0].schema.sub().sub(), ppsr::subject{"orders-value"});
}

TEST_F(
  mirroring_task_test, source_context_listing_failure_counts_and_recovers) {
    _source_state.add(ppsr::context_subject::unqualified("orders-value"), 1);
    // A reachable failure (not source_unavailable, which would park the link)
    // must still be counted while the task stays active.
    _source_state.list_contexts_error = srs::source_error{
      .kind = srs::source_error_kind::operation_failed,
      .message = "context listing failed"};

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto failed = wait_for_sync_status([](const auto& s) {
                      return s.totals_since_task_start.errors >= 1;
                  }).get();
    ASSERT_TRUE(failed.has_value());
    EXPECT_FALSE(failed->last_full_sync.has_value());
    EXPECT_EQ(failed->last_error_message, "context listing failed");

    _source_state.list_contexts_error.reset();
    auto ok = wait_for_sync_status([](const auto& s) {
                  return s.last_full_sync.has_value()
                         && s.inventory.selected_source_subjects == 1;
              }).get();
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(ok->last_full_sync->subject_versions_changed, 1);
}

TEST_F(mirroring_task_test, source_filter_scopes_discovery_and_import) {
    // The source has two default-context subjects; the configured subject
    // filter selects only orders-value. The excluded payments-value must be
    // neither listed (selected_source_*) nor imported (destination).
    auto orders = ppsr::context_subject::unqualified("orders-value");
    auto payments = ppsr::context_subject::unqualified("payments-value");
    _source_state.add(orders, 1);
    _source_state.add(payments, 1);

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()
      ->filter.subjects.push_back("orders-value");

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();

    ASSERT_TRUE(status.has_value());
    // Only the in-scope subject is selected and imported.
    EXPECT_EQ(status->inventory.selected_source_subjects, 1);
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 1);
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1);
    EXPECT_EQ(all[0].schema.sub().sub(), ppsr::subject{"orders-value"});
}

TEST_F(mirroring_task_test, source_filter_excludes_unlisted_context) {
    // The source has a default-context subject and a non-default-context
    // subject. Filtering to the default context must exclude the non-default
    // one from discovery -- and, because it is excluded, the run never needs
    // qualified subjects for it.
    auto orders = ppsr::context_subject::unqualified("orders-value");
    auto other = ppsr::context_subject{
      ppsr::context{".other"}, ppsr::subject{"x"}};
    _source_state.contexts.push_back(ppsr::context{".other"});
    _source_state.add(orders, 1);
    _source_state.add(other, 1);

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()
      ->filter.contexts.push_back(std::string{ppsr::default_context()});

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();

    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->inventory.selected_source_subjects, 1);
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 1);
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1);
    EXPECT_EQ(all[0].schema.sub().ctx, ppsr::default_context);
}

TEST_F(mirroring_task_test, syncs_soft_deleted_source_versions) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    auto payments = ppsr::context_subject::unqualified("payments-value");
    // orders-value: v1 active, v2 soft-deleted. payments-value: only a
    // soft-deleted v1 (no active version at all, so it is reached only because
    // discovery enumerates deleted versions).
    _source_state.add(orders, 1);
    _source_state.add(orders, 2, ppsr::is_deleted::yes);
    _source_state.add(payments, 1, ppsr::is_deleted::yes);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    // The selected-versions count spans active and soft-deleted discoveries
    // (orders v1 + v2, payments v1), not just the active subset.
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 3);

    // All three versions are synced, each preserving its source deleted state.
    const auto& all = _registry.get_all();
    EXPECT_EQ(all.size(), 3);
    auto find_ver = [&](
                      const ppsr::context_subject& sub,
                      int32_t v) -> const ppsr::stored_schema* {
        for (const auto& s : all) {
            if (s.schema.sub() == sub && s.version == ppsr::schema_version{v}) {
                return &s;
            }
        }
        return nullptr;
    };
    const auto* o1 = find_ver(orders, 1);
    const auto* o2 = find_ver(orders, 2);
    const auto* p1 = find_ver(payments, 1);
    ASSERT_NE(o1, nullptr);
    ASSERT_NE(o2, nullptr);
    ASSERT_NE(p1, nullptr);
    EXPECT_EQ(o1->deleted, ppsr::is_deleted::no);
    EXPECT_EQ(o2->deleted, ppsr::is_deleted::yes);
    EXPECT_EQ(p1->deleted, ppsr::is_deleted::yes);
}

TEST_F(
  mirroring_task_test, propagates_source_soft_delete_to_active_destination) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    // The destination holds orders-value:v1 active (a prior sync imported it);
    // the source has since soft-deleted it. The run must propagate the delete
    // by re-importing the deleted body over the live destination version.
    seed_destination("orders-value", 1);
    _source_state.add(orders, 1, ppsr::is_deleted::yes);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status
      = wait_for_sync_status([](const auto& s) {
            return s.last_full_sync.has_value() && !s.current_sync.has_value()
                   && s.totals_since_task_start.subject_versions_changed >= 1;
        }).get();
    ASSERT_TRUE(status.has_value());

    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1);
    EXPECT_EQ(all[0].schema.sub(), orders);
    EXPECT_EQ(all[0].version, ppsr::schema_version{1});
    EXPECT_EQ(all[0].deleted, ppsr::is_deleted::yes);
}

TEST_F(mirroring_task_test, pauses_when_config_paused) {
    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());

    // Pausing the config disables the task. It still leads _schemas/0, so the
    // reconciler pauses it (rather than stopping it, which is
    // placement-driven).
    auto paused = get_default_metadata();
    paused.configuration.schema_registry_sync_cfg.api_mode()->is_enabled
      = model::enabled_t::no;
    fixture()->update_link(model::id_t{0}, std::move(paused)).get();
    ASSERT_TRUE(wait_for_task_state(model::task_state::paused).get());

    // Un-pausing re-enables the task; the reconciler brings it back to active.
    fixture()->update_link(model::id_t{0}, get_default_metadata()).get();
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());
}

TEST_F(mirroring_task_test, follows_partition_leadership) {
    auto subject = ppsr::context_subject::unqualified("orders-value");
    _source_state.add(subject, 1);

    // No leadership on the current node: the task should remain stopped.
    unlead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    ASSERT_TRUE(wait_for_task_state(model::task_state::stopped).get());

    // Acquire leadership: the task should start and become active.
    lead_schema_registry();
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());

    // Lose leadership: the task should stop again.
    unlead_schema_registry();
    ASSERT_TRUE(wait_for_task_state(model::task_state::stopped).get());

    // A stopped (non-leader) shard must not surface SR status, otherwise its
    // empty default could win the admin aggregation over the real leader.
    auto report = fixture()->get_manager().local().get_task_status_report();
    const auto* task = find_sr_status(report);
    ASSERT_NE(task, nullptr);
    EXPECT_FALSE(task->detail.has_value());
}

TEST_F(mirroring_task_test, destination_inventory_spans_contexts_and_deleted) {
    auto a = ppsr::context_subject::unqualified("a");
    auto c = ppsr::context_subject{ppsr::context{".b"}, ppsr::subject{"c"}};
    // Default-context "a": v1 active, v2 soft-deleted. Context ".b" subject
    // "c": v1 active. The scan must span both contexts and separate active from
    // soft-deleted.
    _registry.import_schema(make_schema(a, 1, R"({"v":1})")).get();
    _registry
      .import_schema(make_schema(a, 2, R"({"v":2})", ppsr::is_deleted::yes))
      .get();
    _registry.import_schema(make_schema(c, 1, R"({"v":1})")).get();

    ss::abort_source as;
    auto inv = srs::scan_destination_inventory(
                 _registry,
                 [](const ppsr::context_subject&) { return true; },
                 as)
                 .get();

    auto a_v1 = ppsr::subject_version{a, ppsr::schema_version{1}};
    auto a_v2 = ppsr::subject_version{a, ppsr::schema_version{2}};
    auto c_v1 = ppsr::subject_version{c, ppsr::schema_version{1}};

    EXPECT_THAT(inv.active, testing::UnorderedElementsAre(a_v1, c_v1));
    EXPECT_THAT(inv.all, testing::UnorderedElementsAre(a_v1, c_v1, a_v2));
}

} // namespace cluster_link::tests
