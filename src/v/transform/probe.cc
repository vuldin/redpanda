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

#include "probe.h"

#include "metrics/prometheus_sanitize.h"
#include "model/transform.h"

#include <seastar/core/metrics.hh>

#include <chrono>
#include <optional>

namespace transform {

namespace {

/**
 * How long ago `event_time` was, for a wall-clock timestamp carried on a record
 * batch.
 *
 * Returns nullopt when there is no meaningful delay to report. That covers a
 * missing timestamp, and also a timestamp in the future, which is expected
 * rather than exceptional: when the input topic uses producer-supplied
 * (CreateTime) timestamps we are comparing against a clock that is not ours, so
 * a producer running ahead of us would otherwise contribute a bogus sample.
 */
std::optional<std::chrono::milliseconds>
delay_since(model::timestamp event_time) {
    if (event_time.is_missing() || event_time == model::timestamp::min()) {
        return std::nullopt;
    }
    auto now = model::timestamp::now();
    if (now < event_time) {
        return std::nullopt;
    }
    return std::chrono::milliseconds(now.value() - event_time.value());
}

} // namespace

void probe::setup_metrics(const model::transform_metadata& meta) {
    wasm::transform_probe::setup_metrics(meta.name());
    namespace sm = ss::metrics;

    auto name_label = sm::label("function_name");
    const std::vector<sm::label_instance> labels = {
      name_label(meta.name()),
    };
    std::vector<sm::metric_definition> metric_defs;
    metric_defs.emplace_back(
      sm::make_counter(
        "read_bytes",
        [this] { return _read_bytes; },
        sm::description("The number of bytes input to the transform"),
        labels)
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "failures",
        [this] { return _failures; },
        sm::description("The number of transform failures"),
        labels)
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "batches_given_up",
        [this] { return _given_up; },
        sm::description(
          "The number of batches this transform gave up on after "
          "exhausting transform_failure_policy::max_retries - each was "
          "either skipped or dead-lettered, then the processor advanced "
          "past it instead of stalling the partition"),
        labels)
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "state_recovery_failures",
        [this] { return _state_recovery_failures; },
        sm::description(
          "The number of times this transform had a persisted guest-state "
          "snapshot on this partition that could not be delivered into the "
          "guest's memory on start"),
        labels)
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_histogram(
        "input_delay_seconds",
        sm::description(
          "A histogram of how old a record batch was, in seconds, when the "
          "transform began processing it. For a caught-up transform this is "
          "the delay before it noticed the record; for one working through a "
          "backlog it is how old those records already were, so read it "
          "alongside lag"),
        labels,
        [this] { return _input_delay.public_histogram_logform(); })
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_histogram(
        "e2e_latency_seconds",
        sm::description(
          "A histogram of the total time in seconds from a record batch being "
          "appended to the input topic to the resulting output being written "
          "and its progress committed. Unlike transform_execution_latency_sec "
          "this includes input delay, queueing, and the write path"),
        labels,
        [this] { return _e2e_latency.public_histogram_logform(); })
        .aggregate({sm::shard_label}));

    auto output_topic_label = sm::label("output_topic");
    _lag.reserve(meta.output_topics.size());
    _write_bytes.reserve(meta.output_topics.size());
    for (size_t i = 0; i < meta.output_topics.size(); ++i) {
        _lag.push_back(0);
        _write_bytes.push_back(0);
        std::vector<sm::label_instance> output_topic_labels = labels;
        output_topic_labels.push_back(
          output_topic_label(meta.output_topics[i].tp()));
        metric_defs.emplace_back(
          sm::make_gauge(
            "lag",
            [this, i] { return _lag[i]; },
            sm::description(
              "The number of pending records on the input topic that have "
              "not yet been processed by the transform"),
            output_topic_labels)
            .aggregate({sm::shard_label}));
        metric_defs.emplace_back(
          sm::make_counter(
            "write_bytes",
            [this, i] { return _write_bytes[i]; },
            sm::description("The number of bytes output by the transform"),
            output_topic_labels)
            .aggregate({sm::shard_label}));
    }

    auto state_label = sm::label("state");
    using state = model::transform_report::processor::state;
    for (const auto& s : {state::running, state::inactive, state::errored}) {
        std::vector<sm::label_instance> state_labels = labels;
        state_labels.push_back(
          state_label(model::processor_state_to_string(s)));
        metric_defs.emplace_back(
          sm::make_gauge(
            "state",
            [this, s] { return _processor_state[s]; },
            sm::description("The number of transforms in a specific state"),
            state_labels)
            .aggregate({sm::shard_label}));
    }
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("transform"), metric_defs);
}
void probe::increment_write_bytes(
  model::output_topic_index idx, uint64_t bytes) {
    _write_bytes[idx()] += bytes;
}
void probe::increment_read_bytes(uint64_t bytes) { _read_bytes += bytes; }
void probe::increment_failure() { ++_failures; }
void probe::increment_given_up() { ++_given_up; }
void probe::increment_state_recovery_failure() { ++_state_recovery_failures; }
void probe::state_change(processor_state_change change) {
    if (change.from) {
        _processor_state[*change.from] -= 1;
    }
    if (change.to) {
        _processor_state[*change.to] += 1;
    }
}
void probe::report_lag(model::output_topic_index idx, int64_t delta) {
    _lag.at(idx()) += delta;
}
void probe::record_input_delay(model::timestamp source_batch_timestamp) {
    if (auto delay = delay_since(source_batch_timestamp); delay.has_value()) {
        _input_delay.record(*delay);
    }
}
void probe::record_e2e_latency(model::timestamp source_batch_timestamp) {
    if (auto delay = delay_since(source_batch_timestamp); delay.has_value()) {
        _e2e_latency.record(*delay);
    }
}

} // namespace transform
