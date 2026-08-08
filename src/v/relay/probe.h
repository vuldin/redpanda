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
#include "metrics/metrics.h"
#include "utils/log_hist.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <cstdint>

namespace relay {

/**
 * Per-shard metrics for the relay service. Counters are monotonic; the
 * subscriptions gauge reflects the live count on this shard.
 *
 * The two latency histograms are the only way to tell the relay's two
 * architecturally distinct costs apart, which is why they exist:
 *
 *  - `fanout_duration` is the producer shard's own cost - the synchronous
 *    per-subscriber dispatch loop, where subscriber N waits for the N-1
 *    `deliver()` calls before it. This scales with subscriber count.
 *  - `consume_delay` is the consumer's cost - how long a pushed record sat
 *    enqueued before that consumer's processor fiber was scheduled to take
 *    it. This scales with how many processors share the core.
 *  - `crossshard_dispatch` is the producer shard's cost of fanning out to
 *    OTHER shards: the payload copy plus every `invoke_on` submission. Added
 *    2026-08-29 because `fanout_duration` covers only the local subscriber
 *    loop, so it was timing the cheap half of `push()` - the measured
 *    bottleneck turned out to be the half that was not instrumented.
 *  - `crossshard_transit` is the wall time a record spends BETWEEN those two:
 *    from the producer shard finishing its submissions to the record arriving
 *    on a destination shard. Added 2026-09-01 to close the one gap left in the
 *    timeline. `crossshard_dispatch` deliberately stops before awaiting
 *    anything (it measures how long the matcher was held up), and
 *    `consume_delay` starts only once the record is already enqueued on the
 *    destination, so time spent queued in seastar's cross-shard path was
 *    charged to no stage at all. That matters because the fan-out ceiling has
 *    twice been misattributed: first to CPU saturation, then to cross-shard
 *    submission backpressure - which cannot exist, since the default smp
 *    service group's per-destination semaphores are unbounded
 *    (`max_counter()`). Overload there shows up as an unboundedly growing
 *    source-side `pending_fifo`, and seastar has NO metric for that queue's
 *    depth (its `smp` group is all `metric_disabled`, and the one gauge that
 *    survives, `send_queue_length`, saturates at 128). Measuring the transit
 *    latency directly is strictly more informative than the queue depth would
 *    have been, and needs no seastar change.
 *
 *    Recorded on the DESTINATION shard, so like `consume_delay` it is charged
 *    where the waiting happened rather than where the push originated. Safe to
 *    compare against the other three: `ss::steady_clock_type` is
 *    CLOCK_MONOTONIC, consistent across cores, so this is single-clock.
 *
 * Without them, both are attributed to whatever the benchmark calls
 * "relay_consume", and a regression in one is indistinguishable from a
 * regression in the other. See bench/RELAY-LATENCY-STAGES.md in
 * wasm-orderbook, which specced these before they were built.
 *
 * Both are gated on `relay_stage_metrics_enabled` (default false) because
 * each costs a steady-clock read on the relay hot path.
 */
class probe {
public:
    probe() = default;
    probe(const probe&) = delete;
    probe& operator=(const probe&) = delete;
    probe(probe&&) = delete;
    probe& operator=(probe&&) = delete;
    ~probe() = default;

    void setup_metrics();

    // log_hist_internal, NOT log_hist_public: public storage buckets everything
    // below 256us together, which is every value this probe measures.
    using hist_t = log_hist_internal;

    void increment_pushes() { ++_pushes; }
    void increment_delivered() { ++_delivered; }
    void increment_dropped() { ++_dropped; }
    void subscription_added() { ++_subscriptions; }
    void subscription_removed() { --_subscriptions; }

    // Both take an already-computed duration rather than a start time: the
    // caller owns the clock read so it can skip it entirely when the
    // feature is off, which is the whole point of gating this.
    void record_fanout_duration(std::chrono::microseconds d) {
        _fanout_duration.record(static_cast<uint64_t>(d.count()));
    }
    void record_consume_delay(std::chrono::microseconds d) {
        _consume_delay.record(static_cast<uint64_t>(d.count()));
    }
    void record_crossshard_dispatch(std::chrono::microseconds d) {
        _crossshard_dispatch.record(static_cast<uint64_t>(d.count()));
    }
    void record_crossshard_transit(std::chrono::microseconds d) {
        _crossshard_transit.record(static_cast<uint64_t>(d.count()));
    }
    /// emit -> consuming guest dequeue, the whole in-broker fan-out path in ONE
    /// span. Prefer this over summing dispatch+transit+fanout+consume: those
    /// overlap (transit starts inside dispatch's window) and leave gaps between
    /// stages, so the sum is neither an upper nor a lower bound.
    void record_emit_to_guest(std::chrono::microseconds d) {
        _emit_to_guest.record(static_cast<uint64_t>(d.count()));
    }

    // Read-back accessors, exposed for tests (the metrics themselves are the
    // production surface).
    uint64_t pushes() const { return _pushes; }
    uint64_t delivered() const { return _delivered; }
    uint64_t dropped() const { return _dropped; }
    int64_t subscriptions() const { return _subscriptions; }
    const hist_t& fanout_duration() const { return _fanout_duration; }
    const hist_t& consume_delay() const { return _consume_delay; }
    const hist_t& crossshard_dispatch() const { return _crossshard_dispatch; }
    const hist_t& crossshard_transit() const { return _crossshard_transit; }
    const hist_t& emit_to_guest() const { return _emit_to_guest; }

private:
    uint64_t _pushes = 0;
    uint64_t _delivered = 0;
    uint64_t _dropped = 0;
    int64_t _subscriptions = 0;
    hist_t _fanout_duration;
    hist_t _consume_delay;
    hist_t _crossshard_dispatch;
    hist_t _crossshard_transit;
    hist_t _emit_to_guest;
    metrics::public_metric_groups _public_metrics;
};

} // namespace relay
