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

    using hist_t = log_hist_public;

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

    // Read-back accessors, exposed for tests (the metrics themselves are the
    // production surface).
    uint64_t pushes() const { return _pushes; }
    uint64_t delivered() const { return _delivered; }
    uint64_t dropped() const { return _dropped; }
    int64_t subscriptions() const { return _subscriptions; }
    const hist_t& fanout_duration() const { return _fanout_duration; }
    const hist_t& consume_delay() const { return _consume_delay; }
    const hist_t& crossshard_dispatch() const { return _crossshard_dispatch; }

private:
    uint64_t _pushes = 0;
    uint64_t _delivered = 0;
    uint64_t _dropped = 0;
    int64_t _subscriptions = 0;
    hist_t _fanout_duration;
    hist_t _consume_delay;
    hist_t _crossshard_dispatch;
    metrics::public_metric_groups _public_metrics;
};

} // namespace relay
