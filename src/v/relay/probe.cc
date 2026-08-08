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
          sm::description("Number of live relay subscriptions on this shard"))
          .aggregate({ss::metrics::shard_label}),
        // Always registered, even when relay_stage_metrics_enabled is off -
        // the property gates *recording*, not registration, so flipping it
        // live starts populating an already-scraped series instead of making
        // one appear and break whatever is graphing it. An untouched
        // histogram reports zero count, which reads correctly as "not
        // measured".
        sm::make_histogram(
          "fanout_duration_seconds",
          sm::description(
            "A histogram of how long the relay's synchronous per-subscriber "
            "dispatch loop took for one push, on the producer's shard. Scales "
            "with subscriber count. Only recorded when "
            "relay_stage_metrics_enabled is set."),
          [this] { return _fanout_duration.relay_histogram_logform(); })
          .aggregate({ss::metrics::shard_label}),
        sm::make_histogram(
          "crossshard_dispatch_duration_seconds",
          sm::description(
            "A histogram of how long the producer's shard spent fanning a "
            "single push out to OTHER shards: one payload copy plus every "
            "cross-shard submission. This is charged to the producing "
            "transform's critical path, unlike consume_delay. Only recorded "
            "when relay_stage_metrics_enabled is set."),
          [this] { return _crossshard_dispatch.relay_histogram_logform(); })
          .aggregate({ss::metrics::shard_label}),
        sm::make_histogram(
          "crossshard_transit_duration_seconds",
          sm::description(
            "A histogram of how long a record spent in seastar's cross-shard "
            "path: from the producer shard finishing its submissions to the "
            "record arriving on this destination shard. Fills the gap between "
            "crossshard_dispatch (which stops before awaiting) and "
            "consume_delay (which starts once already enqueued here). "
            "Recorded on the destination shard. Only recorded when "
            "relay_stage_metrics_enabled is set."),
          [this] { return _crossshard_transit.relay_histogram_logform(); })
          .aggregate({ss::metrics::shard_label}),
        sm::make_histogram(
          "emit_to_guest_duration_seconds",
          sm::description(
            "A histogram of the WHOLE in-broker fan-out path for one record: "
            "from the producing transform's emit (relay push) through to the "
            "consuming guest dequeuing it. This is the single span to quote - "
            "summing dispatch+transit+fanout_duration+consume_delay instead "
            "double-counts (transit begins inside dispatch's window) and can "
            "miss time falling between stages. Only recorded when "
            "relay_stage_metrics_enabled is set."),
          [this] { return _emit_to_guest.relay_histogram_logform(); })
          .aggregate({ss::metrics::shard_label}),
        sm::make_histogram(
          "consume_delay_seconds",
          sm::description(
            "A histogram of how long a pushed record sat in a relay "
            "consumer's queue before that consumer's processor was scheduled "
            "to read it. This is consumer scheduling delay, not relay "
            "transit. Only recorded when relay_stage_metrics_enabled is set."),
          [this] { return _consume_delay.relay_histogram_logform(); })
          .aggregate({ss::metrics::shard_label}),
      });
}

} // namespace relay
