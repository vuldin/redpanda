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

#include <cstdint>

namespace relay {

/**
 * Per-shard metrics for the relay service. Counters are monotonic; the
 * subscriptions gauge reflects the live count on this shard.
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

    void increment_pushes() { ++_pushes; }
    void increment_delivered() { ++_delivered; }
    void increment_dropped() { ++_dropped; }
    void subscription_added() { ++_subscriptions; }
    void subscription_removed() { --_subscriptions; }

    // Read-back accessors, exposed for tests (the metrics themselves are the
    // production surface).
    uint64_t pushes() const { return _pushes; }
    uint64_t delivered() const { return _delivered; }
    uint64_t dropped() const { return _dropped; }
    int64_t subscriptions() const { return _subscriptions; }

private:
    uint64_t _pushes = 0;
    uint64_t _delivered = 0;
    uint64_t _dropped = 0;
    int64_t _subscriptions = 0;
    metrics::public_metric_groups _public_metrics;
};

} // namespace relay
