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

#pragma once

#include "base/outcome.h"
#include "cluster/errc.h"
#include "cluster/state_machine_registry.h"
#include "config/property.h"
#include "model/transform.h"
#include "raft/persisted_stm.h"

#include <absl/container/flat_hash_map.h>

namespace transform {

/**
 * Durably stores the most recent guest-state snapshot for each transform
 * deployed against this exact partition. Without this, guest state is
 * silently zeroed on every error/leadership move/redeploy, then partially -
 * and wrongly - rebuilt from a few seconds of input replay.
 *
 * Deliberately attached directly to the input topic's own raft group,
 * rather than routed through a separate internal topic the way
 * transform_offsets_stm_t is (see that type's coordinator/hash-routing
 * indirection): a snapshot is only ever useful to whichever node is, or
 * becomes, leader for *this* ntp, so piggybacking on the replication this
 * partition's own raft group already does needs no coordinator at all, and
 * gets snapshot delivery to a new leader for free via the same
 * log-replication and raft-snapshot-install path that already catches that
 * new leader up on the real data. is_applicable_for attaches this to every
 * user-topic partition unconditionally - the same posture
 * cluster::partition_properties_stm already takes - cheap when unused,
 * since do_apply/take_*_snapshot are trivial until put_state is ever
 * actually called for some transform_id.
 *
 * Uses get_initial_recovery_policy() == skip_to_end (same as
 * partition_properties_stm), so a freshly-created replica does NOT replay
 * historical transform_state_update batches from the log - it relies
 * entirely on the local/raft snapshot to learn `_state`. That's only safe
 * because take_raft_snapshot ignores the offset it's asked for and just
 * serializes whatever `_state` currently is: since do_apply is a pure
 * last-writer-wins overwrite (or, for a tombstone, an idempotent erase),
 * handing a replica a snapshot that's "ahead of" the offset the framework
 * labels it with is harmless - replaying the (idempotent) log entries
 * between that label and the true current offset on top of it converges to
 * the same final state regardless. A data model that wasn't a pure
 * overwrite (e.g. anything additive/incremental) could not make this same
 * simplification.
 */
class transform_state_stm final
  : public raft::persisted_stm<raft::kvstore_backed_stm_snapshot> {
public:
    static constexpr std::string_view name = "transform_state_stm";

    transform_state_stm(
      raft::consensus*,
      ss::logger&,
      storage::kvstore&,
      config::binding<size_t> max_snapshot_size);

    /**
     * Persist `state` as the latest snapshot for `id` on this partition,
     * replacing whatever was stored before. Resolves once replicated to a
     * quorum of this partition's own raft group.
     *
     * Rejects (rather than truncating) a snapshot larger than
     * data_transforms_state_snapshot_max_size - the whole point of failing
     * loudly on restore is defeated if a silently-truncated snapshot is
     * what gets restored.
     */
    ss::future<result<model::offset, cluster::errc>>
    put_state(model::transform_id, iobuf state, model::timeout_clock::duration);

    /**
     * Remove a previously-put snapshot for `id`, e.g. once its transform is
     * undeployed and the blob would otherwise be orphaned on this partition
     * forever. A no-op (success) if nothing was stored for `id`.
     */
    ss::future<result<model::offset, cluster::errc>>
      remove_state(model::transform_id, model::timeout_clock::duration);

    /**
     * The most recently persisted snapshot for `id` on this partition, or
     * std::nullopt if none has ever been persisted - callers cannot tell
     * "never persisted" (expected for a brand-new deploy) from "persisted,
     * then lost" from this alone; see transform_processor.cc's restore
     * path, which combines this with the transform's own deploy-time
     * options to make that call.
     *
     * Syncs first, so a freshly-elected leader doesn't answer from stale
     * pre-election state.
     */
    ss::future<result<std::optional<iobuf>, cluster::errc>>
      sync_latest_state(model::transform_id, model::timeout_clock::duration);

    ss::future<iobuf> take_raft_snapshot(model::offset) final;

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        // Only the latest snapshot per id is ever kept (see do_apply) -
        // there's no history to gain from replaying the whole log, only
        // startup latency to lose. Same justification
        // partition_properties_stm already uses.
        return raft::stm_initial_recovery_policy::skip_to_end;
    }

protected:
    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;

    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units apply_units) override;

private:
    ss::future<> do_apply(const model::record_batch&) final;
    ss::future<> apply_raft_snapshot(const iobuf&) final;
    void apply_record(model::record);

    static model::record_batch make_batch(model::transform_id, iobuf state);

    ss::future<result<model::offset, cluster::errc>>
      replicate(model::record_batch, model::timeout_clock::duration);

    config::binding<size_t> _max_snapshot_size;

    // Only the latest value per id is kept, deliberately - this is a
    // snapshot store, not a changelog. Small enough in the common case (a
    // handful of transforms per partition, each capped at
    // data_transforms_state_snapshot_max_size) to keep entirely in memory,
    // the same assumption distributed_kv_stm makes for its own,
    // differently-shaped values. An empty (zero-byte) value is a tombstone
    // (see apply_record) rather than a legitimate stored state - a guest
    // with genuinely zero bytes of state has nothing worth recovering
    // anyway, so this is unambiguous.
    absl::flat_hash_map<model::transform_id, iobuf> _state;
    ssx::mutex _write_mutex{"transform::transform_state_stm::write"};
};

class transform_state_stm_factory : public cluster::state_machine_factory {
public:
    transform_state_stm_factory(
      storage::kvstore&, config::binding<size_t> max_snapshot_size);

    bool is_applicable_for(const storage::ntp_config&) const final;

    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const cluster::stm_instance_config&) final;

private:
    storage::kvstore& _kvstore;
    config::binding<size_t> _max_snapshot_size;
};

} // namespace transform
