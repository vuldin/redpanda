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

    bool on_push(const iobuf& payload);

    relay::service* _relay;
    model::ntp _relay_ntp;
    std::unique_ptr<queue_subscription> _sub;
    uint32_t _sub_id = 0;

    ss::chunked_fifo<model::transformed_data> _pending;
    // Total messages delivered to this source since start - the synthetic
    // offset space. Monotonic, per-push.
    int64_t _delivered_count = 0;
    // Base offset to stamp on the next batch read_batch produces.
    kafka::offset _next_offset{0};
    ssx::condition_variable _data_available;
    ss::gate _gate;
};

} // namespace transform
