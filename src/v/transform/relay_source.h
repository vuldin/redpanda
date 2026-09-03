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

#include "base/seastarx.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/transform.h"
#include "relay/fwd.h"
#include "ssx/condition_variable.h"
#include "transform/io.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <memory>
#include <optional>

namespace transform {

/**
 * A transform source fed by the relay instead of a partition read - the
 * in-broker counterpart to the relay's external TCP consumers. A wasm
 * consumer deployed with this source subscribes to the relay for its
 * (input topic, input partition) and is driven by relay pushes rather than
 * by reading the log, so many in-broker consumers of one producer's output
 * don't each pay an independent partition read.
 *
 * Shard-locality: the producing transform pushes to the relay on the shard
 * it runs on, so a relay-sourced consumer only receives data when it is
 * scheduled on that same shard. This is a v1 constraint; cross-shard relay
 * is future work.
 *
 * Delivery is best-effort (the relay drops for a backlogged consumer) and
 * offsets are synthetic (monotonic, per-push) - a relay consumer gets
 * correctness from the durable output topic, which is still written, not
 * from the relay. Gap detection + resync-from-durable-topic is the planned
 * follow-up.
 */
class relay_source final : public source {
public:
    relay_source(relay::service* relay, model::ntp relay_ntp);
    relay_source(const relay_source&) = delete;
    relay_source& operator=(const relay_source&) = delete;
    relay_source(relay_source&&) = delete;
    relay_source& operator=(relay_source&&) = delete;
    // Out-of-line: queue_subscription is only complete in the .cc, and the
    // unique_ptr member's deleter needs the complete type.
    ~relay_source() final;

    ss::future<> start() final;
    ss::future<> stop() final;

    // The offset the next delivered-but-unconsumed message will carry.
    kafka::offset latest_offset() final;

    // The relay has no durable log to timequery - unsupported, returns
    // nullopt.
    ss::future<std::optional<kafka::offset>>
    offset_at_timestamp(model::timestamp, ss::abort_source*) final;

    // Relay consumers only ever see data pushed after they subscribe, so the
    // "start" of the stream is the subscription point.
    kafka::offset start_offset() const final;

    ss::future<model::record_batch_reader>
    read_batch(kafka::offset, ss::abort_source*) final;

    ss::future<> wait_for_offset(
      kafka::offset offset,
      model::timeout_clock::time_point deadline,
      ss::abort_source*) final;

private:
    // The relay::subscription we register with the relay; deliver() enqueues
    // onto this source's pending queue.
    class queue_subscription;

    bool on_push(
      const iobuf& data,
      ss::steady_clock_type::time_point pushed_at
      = ss::steady_clock_type::time_point{});

    relay::service* _relay;
    model::ntp _relay_ntp;
    std::unique_ptr<queue_subscription> _sub;
    uint32_t _sub_id = 0;

    // Bounded the same way tcp_subscription bounds its own queue (see
    // relay::service::max_queue_size()) - a relay-sourced consumer that
    // falls behind drops the oldest pending record instead of growing
    // without limit, matching the relay's best-effort delivery contract
    // (relay::subscription's own doc comment).
    size_t _max_pending;

    // The enqueue timestamp travels with the record so the enqueue-to-dequeue
    // delay can be measured in read_batch. Kept in the same queue rather than
    // a parallel one deliberately: on_push drops the OLDEST entry when
    // backlogged, and two containers that must stay index-aligned across that
    // drop is exactly the kind of bookkeeping that silently desyncs. Costs 8
    // bytes per queued record (bounded by _max_pending, default 1024).
    //
    // Left default-constructed when relay_stage_metrics_enabled is off, which
    // read_batch treats as "don't record" - so flipping the property live
    // cannot produce a garbage sample from a record enqueued while it was off.
    struct pending_record {
        model::transformed_data data;
        ss::steady_clock_type::time_point enqueued_at;
        // The producing transform's emit instant, carried from relay::push so
        // read_batch can close the emit -> guest-dequeue span in one
        // measurement instead of summing overlapping stage averages.
        ss::steady_clock_type::time_point pushed_at;
    };
    ss::chunked_fifo<pending_record> _pending;
    // Total messages delivered to this source since start - the synthetic
    // offset space. Monotonic, per-push.
    int64_t _delivered_count = 0;
    // Base offset to stamp on the next batch read_batch produces.
    kafka::offset _next_offset{0};
    // Optional so both can be reset on every start() - a processor
    // restarts by calling stop() then start() again on this SAME
    // relay_source instance (see transform_manager.cc's
    // start_processor/handle_transform_error), not by constructing a
    // fresh one. A plain ss::gate/condition_variable is a one-shot
    // object: close()/broken() permanently ends it, so a second
    // restart cycle without emplace() here would call close() on an
    // already-closed gate - confirmed for real, it aborts the whole
    // broker (SEASTAR_ASSERT, not compiled out in release). Same
    // pattern transform_module.h already uses for its own guest/host
    // condition variables, for the same reason.
    std::optional<ssx::condition_variable> _data_available;
    std::optional<ss::gate> _gate;
};

} // namespace transform
