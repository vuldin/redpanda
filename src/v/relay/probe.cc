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

#include "relay/probe.h"

#include "metrics/prometheus_sanitize.h"

namespace relay {

void probe::setup_metrics() {
    namespace sm = ss::metrics;
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("relay"),
      {
        sm::make_counter(
          "pushes_total",
          [this] { return _pushes; },
          sm::description(
            "Total number of pushes received from producers on this shard"))
          .aggregate({ss::metrics::shard_label}),
        sm::make_counter(
          "delivered_total",
          [this] { return _delivered; },
          sm::description(
            "Total number of records successfully handed to a relay "
            "consumer on this shard"))
          .aggregate({ss::metrics::shard_label}),
        sm::make_counter(
          "dropped_total",
          [this] { return _dropped; },
          sm::description(
            "Total number of records dropped because a relay consumer was "
            "backlogged on this shard"))
          .aggregate({ss::metrics::shard_label}),
        sm::make_gauge(
          "active_subscriptions",
          [this] { return _subscriptions; },
          sm::description(
            "Number of live relay subscriptions on this shard"))
          .aggregate({ss::metrics::shard_label}),
      });
}

} // namespace relay
