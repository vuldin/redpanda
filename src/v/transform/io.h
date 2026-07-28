/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/transform.h"

namespace transform {

/**
 * The output sink for Wasm transforms.
 */
class sink {
public:
    sink() = default;
    sink(const sink&) = delete;
    sink& operator=(const sink&) = delete;
    sink(sink&&) = delete;
    sink& operator=(sink&&) = delete;
    virtual ~sink() = default;

    /**
     * `partition_key`, if set, is a guest-chosen key that this write should
     * be routed by, consistent with normal Kafka key-based partitioning -
     * instead of this sink's default routing (today,
     * same-index-as-input-partition).
     */
    virtual ss::future<> write(
      ss::chunked_fifo<model::record_batch>,
      std::optional<iobuf> partition_key) = 0;
};

/**
 * The input source for Wasm transforms.
 */
class source {
public:
    source() = default;
    source(const source&) = delete;
    source& operator=(const source&) = delete;
    source(source&&) = delete;
    source& operator=(source&&) = delete;
    virtual ~source() = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    /**
     * The last offset of a record the log - if the log is empty then
     * `kafka::offset::min()` is returned.
     */
    virtual kafka::offset latest_offset() = 0;

    /**
     * The offset of a record the log for a given timestamp - if the log is
     * empty or the timestamp is greater than max_timestamp, then std::nullopt
     * is returned.
     */
    virtual ss::future<std::optional<kafka::offset>>
    offset_at_timestamp(model::timestamp, ss::abort_source*) = 0;

    /**
     * The minimum offset in the source log
     */
    virtual kafka::offset start_offset() const = 0;

    /**
     * Read from the log starting at a given offset, aborting when requested.
     *
     * NOTE: It's important in terms of lifetimes that the source **always**
     * outlives any reader returned from this method.
     *
     * NOTE: It's not valid to have pending futures outstanding from this
     * method before calling stop.
     */
    virtual ss::future<model::record_batch_reader>
    read_batch(kafka::offset, ss::abort_source*) = 0;

    /**
     * Wait until `offset` is likely to have become visible, up until
     * `deadline`, or until aborted.
     *
     * This is a hint, not a guarantee: implementations may return before
     * `offset` is actually visible - for example on timeout, or if they have
     * no better signal to offer than that - so callers must always re-check
     * via `read_batch` after this resolves rather than assuming data is
     * present. It exists so that a source backed by local Raft replication
     * can notify promptly when new data commits, instead of forcing every
     * caller to poll on a fixed interval regardless of how idle the source
     * actually is.
     */
    virtual ss::future<> wait_for_offset(
      kafka::offset offset,
      model::timeout_clock::time_point deadline,
      ss::abort_source*) = 0;
};

/**
 * Transforms are at least once delivery, which we achieve by committing
 * progress on the input topic offset periodically.
 */
class offset_tracker {
public:
    offset_tracker() = default;
    offset_tracker(const offset_tracker&) = delete;
    offset_tracker(offset_tracker&&) = delete;
    offset_tracker& operator=(const offset_tracker&) = delete;
    offset_tracker& operator=(offset_tracker&&) = delete;
    virtual ~offset_tracker() = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    /**
     * Load the latest offset for all output topics we've committed.
     */
    virtual ss::future<
      absl::flat_hash_map<model::output_topic_index, kafka::offset>>
    load_committed_offsets() = 0;

    /**
     * Commit progress for a given output topic. The offset here is how far on
     * the input partition we've transformed and successfully written to the
     * output topic.
     */
    virtual ss::future<>
      commit_offset(model::output_topic_index, kafka::offset) = 0;
};

/**
 * Durable storage for a transform guest's own state (e.g. an order book),
 * used to survive restarts, leadership moves, and redeploys without
 * silently running with wrong or zeroed state. Backed by
 * transform::transform_state_stm, attached
 * directly to this same partition's own raft group - see that class's own
 * doc comment for why.
 */
class state_store {
public:
    state_store() = default;
    state_store(const state_store&) = delete;
    state_store(state_store&&) = delete;
    state_store& operator=(const state_store&) = delete;
    state_store& operator=(state_store&&) = delete;
    virtual ~state_store() = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    /**
     * The most recently persisted guest-state snapshot, or std::nullopt if
     * none has ever been persisted - the latter is the normal, expected
     * answer for a brand-new deploy, not a failure signal. Throws on a
     * genuine storage-layer error (e.g. this partition's state STM isn't
     * reachable) - callers should let that propagate the same way any
     * other processor start() failure does.
     */
    virtual ss::future<std::optional<iobuf>> load_latest_state() = 0;

    /**
     * Durably persist `state`, replacing whatever was stored before.
     * Resolves once replicated to a quorum. This is a best-effort
     * background durability improvement, not on the critical path of
     * processing any one batch - implementations should not throw on
     * failure, just log and let the next periodic checkpoint retry.
     */
    virtual ss::future<> save_state(iobuf state) = 0;
};

} // namespace transform
