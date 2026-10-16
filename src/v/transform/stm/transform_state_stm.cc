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

#include "transform/stm/transform_state_stm.h"

#include "config/configuration.h"

#include "cluster/errc.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "raft/consensus.h"
#include "serde/rw/rw.h"
#include "storage/record_batch_builder.h"

#include <seastar/util/log.hh>

using namespace std::chrono_literals;

namespace transform {

namespace {
// NOLINTNEXTLINE
ss::logger stm_log{"transform/state_stm"};

struct state_entry
  : serde::envelope<state_entry, serde::version<0>, serde::compat_version<0>> {
    model::transform_id id;
    iobuf state;

    auto serde_fields() { return std::tie(id, state); }
};

struct local_snapshot
  : serde::
      envelope<local_snapshot, serde::version<0>, serde::compat_version<0>> {
    std::vector<state_entry> entries;

    auto serde_fields() { return std::tie(entries); }
};

} // namespace

transform_state_stm::transform_state_stm(
  raft::consensus* raft,
  ss::logger& logger,
  storage::kvstore& kvstore,
  config::binding<size_t> max_snapshot_size)
  : raft::persisted_stm<raft::kvstore_backed_stm_snapshot>(
      "transform_state_stm.snapshot", logger, raft, kvstore)
  , _max_snapshot_size(std::move(max_snapshot_size))
  , _next_generation(
      static_cast<uint64_t>(model::timestamp::now().value()) << 16) {}

ss::future<result<model::offset, cluster::errc>> transform_state_stm::put_state(
  model::transform_id id, iobuf state, model::timeout_clock::duration timeout) {
    if (state.size_bytes() > _max_snapshot_size()) {
        vlog(
          stm_log.warn,
          "rejecting oversized guest-state snapshot for transform {}: {} "
          "bytes exceeds the {} byte limit",
          id,
          state.size_bytes(),
          _max_snapshot_size());
        co_return cluster::errc::invalid_request;
    }
    auto units = co_await _write_mutex.get_units();
    if (!co_await sync(timeout)) {
        co_return cluster::errc::not_leader;
    }
    const size_t chunk
      = config::shard_local_cfg().data_transforms_state_checkpoint_chunk_bytes();
    if (chunk == 0 || state.size_bytes() <= chunk) {
        co_return co_await replicate(make_batch(id, std::move(state)), timeout);
    }
    // One record per chunk, each replicated before the next is issued, so
    // the partition's own produces interleave between them. The write mutex
    // keeps another checkpoint's chunks from interleaving with these.
    vlog(
      stm_log.debug,
      "checkpoint for transform {}: {} bytes as {} chunks of <= {} bytes",
      id,
      state.size_bytes(),
      (state.size_bytes() + chunk - 1) / chunk,
      chunk);
    auto parts = split_state(std::move(state), chunk);
    state_chunk_info info{
      .generation = _next_generation++,
      .index = 0,
      .count = static_cast<uint32_t>(parts.size())};
    model::offset last;
    for (auto& part : parts) {
        auto r = co_await replicate(
          make_chunk_batch(id, info, std::move(part)), timeout);
        if (r.has_error()) {
            co_return r;
        }
        last = r.value();
        ++info.index;
    }
    co_return last;
}

ss::future<result<model::offset, cluster::errc>>
transform_state_stm::remove_state(
  model::transform_id id, model::timeout_clock::duration timeout) {
    auto units = co_await _write_mutex.get_units();
    if (!co_await sync(timeout)) {
        co_return cluster::errc::not_leader;
    }
    if (!_state.contains(id)) {
        co_return last_applied_offset();
    }
    // An empty value is the tombstone - see apply_record.
    co_return co_await replicate(make_batch(id, iobuf{}), timeout);
}

ss::future<result<std::optional<iobuf>, cluster::errc>>
transform_state_stm::sync_latest_state(
  model::transform_id id, model::timeout_clock::duration timeout) {
    auto holder = _gate.hold();
    if (!co_await sync(timeout)) {
        co_return cluster::errc::not_leader;
    }
    auto it = _state.find(id);
    if (it == _state.end()) {
        co_return std::optional<iobuf>(std::nullopt);
    }
    co_return std::optional<iobuf>(it->second.copy());
}

ss::future<result<model::offset, cluster::errc>> transform_state_stm::replicate(
  model::record_batch batch, model::timeout_clock::duration timeout) {
    auto holder = _gate.hold();
    raft::replicate_options r_opts(
      raft::consistency_level::quorum_ack,
      _insync_term,
      std::chrono::milliseconds(timeout / 1ms));
    // No force flush. A guest-state checkpoint describes records already in
    // this partition's log, so it needs no stronger durability than those
    // records got - under write caching, a quorum ack held in memory. Forcing
    // a flush here fsynced the whole partition log every checkpoint, and with
    // write caching on that put every client produce on the partition behind
    // it: the produce-leg p99.9 sat at ~3-9ms with snapshots on (2026-10-16,
    // depth 10,000) against ~1ms without. The original forced flush is kept
    // behind data_transforms_state_checkpoint_inline for comparison.
    if (config::shard_local_cfg().data_transforms_state_checkpoint_inline()) {
        r_opts.set_force_flush();
    }
    auto r = co_await _raft->replicate(std::move(batch), r_opts);
    if (r.has_error()) {
        vlog(
          stm_log.warn,
          "error replicating guest-state snapshot update: {}",
          r.error().message());
        co_await _raft->step_down("transform_state_stm/replication_error");
        co_return cluster::errc::replication_error;
    }
    auto offset = r.value().last_offset;
    if (!co_await wait_no_throw(
          offset, model::timeout_clock::time_point::max())) {
        co_await _raft->step_down("transform_state_stm/replication_error");
        co_return cluster::errc::shutting_down;
    }
    co_return offset;
}

model::record_batch
transform_state_stm::make_batch(model::transform_id id, iobuf state) {
    storage::record_batch_builder builder(
      model::record_batch_type::transform_state_update, model::offset{});
    builder.add_raw_kv(serde::to_iobuf(id), std::move(state));
    return std::move(builder).build();
}

model::record_batch transform_state_stm::make_chunk_batch(
  model::transform_id id, const state_chunk_info& info, iobuf part) {
    storage::record_batch_builder builder(
      model::record_batch_type::transform_state_update, model::offset{});
    chunked_vector<model::record_header> headers;
    auto k = iobuf::from(state_chunk_header_key);
    auto v = serde::to_iobuf(info);
    auto ksz = static_cast<int32_t>(k.size_bytes());
    auto vsz = static_cast<int32_t>(v.size_bytes());
    headers.emplace_back(ksz, std::move(k), vsz, std::move(v));
    builder.add_raw_kw(serde::to_iobuf(id), std::move(part), std::move(headers));
    return std::move(builder).build();
}

ss::future<> transform_state_stm::do_apply(const model::record_batch& b) {
    if (b.header().type != model::record_batch_type::transform_state_update) {
        co_return;
    }
    b.for_each_record([this](model::record r) { apply_record(std::move(r)); });
}

void transform_state_stm::apply_record(model::record r) {
    auto chunk = state_chunk_of(r);
    auto id = serde::from_iobuf<model::transform_id>(r.release_key());
    auto value = r.release_value();
    if (chunk) {
        if (auto whole = _chunks.add(id, *chunk, std::move(value))) {
            _state.insert_or_assign(id, std::move(*whole));
        }
        return;
    }
    _chunks.forget(id);
    if (value.empty()) {
        // Tombstone - see remove_state.
        _state.erase(id);
        return;
    }
    _state.insert_or_assign(id, std::move(value));
}

ss::future<> transform_state_stm::apply_raft_snapshot(const iobuf& buffer) {
    _state.clear();
    _chunks.clear();
    if (buffer.empty()) {
        co_return;
    }
    auto snap = serde::from_iobuf<local_snapshot>(buffer.copy());
    for (auto& entry : snap.entries) {
        _state.insert_or_assign(entry.id, std::move(entry.state));
    }
}

ss::future<iobuf> transform_state_stm::take_raft_snapshot(model::offset) {
    local_snapshot snap;
    snap.entries.reserve(_state.size());
    for (const auto& [id, state] : _state) {
        snap.entries.push_back(state_entry{.id = id, .state = state.copy()});
    }
    co_return serde::to_iobuf(std::move(snap));
}

ss::future<raft::local_snapshot_applied>
transform_state_stm::apply_local_snapshot(
  raft::stm_snapshot_header, iobuf&& buffer) {
    _state.clear();
    _chunks.clear();
    auto snap = serde::from_iobuf<local_snapshot>(std::move(buffer));
    for (auto& entry : snap.entries) {
        _state.insert_or_assign(entry.id, std::move(entry.state));
    }
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
transform_state_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    auto last_applied = last_applied_offset();
    local_snapshot snap;
    snap.entries.reserve(_state.size());
    for (const auto& [id, state] : _state) {
        snap.entries.push_back(state_entry{.id = id, .state = state.copy()});
    }
    apply_units.return_all();
    co_return raft::stm_snapshot::create(
      0, last_applied, serde::to_iobuf(std::move(snap)));
}

transform_state_stm_factory::transform_state_stm_factory(
  storage::kvstore& kvstore, config::binding<size_t> max_snapshot_size)
  : _kvstore(kvstore)
  , _max_snapshot_size(std::move(max_snapshot_size)) {}

bool transform_state_stm_factory::is_applicable_for(
  const storage::ntp_config& cfg) const {
    return model::is_user_topic(cfg.ntp());
}

void transform_state_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<transform_state_stm>(
      raft, stm_log, _kvstore, _max_snapshot_size);
    raft->log()->stm_hookset()->add_stm(stm);
}

} // namespace transform
