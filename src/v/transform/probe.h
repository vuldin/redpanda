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

#include "absl/container/flat_hash_map.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "wasm/transform_probe.h"

namespace transform {

struct processor_state_change {
    using state = model::transform_report::processor::state;

    std::optional<state> from;
    std::optional<state> to;
};

/** A per transform probe. */
class probe : public wasm::transform_probe {
public:
    void setup_metrics(const model::transform_metadata&);

    void increment_read_bytes(uint64_t bytes);
    void increment_write_bytes(model::output_topic_index, uint64_t bytes);
    void increment_failure();
    void state_change(processor_state_change);
    void report_lag(model::output_topic_index, int64_t delta);

    /**
     * Record how old a batch was when the processor began working on it, from
     * that batch's max timestamp.
     *
     * Read this together with lag. A caught-up transform is only as old as the
     * delay before it noticed the record, so this isolates input-side delay
     * from time spent doing work. A transform working through a backlog is
     * instead reading records that were already old when it got to them, so
     * this reports that age rather than any slowness on our part.
     */
    void record_input_delay(model::timestamp source_batch_timestamp);

    /**
     * Record the total time from a batch being appended to the input topic to
     * the output derived from it being written and its progress committed, from
     * that batch's max timestamp.
     *
     * This covers every stage of the pipeline, unlike
     * `transform_execution_latency_sec`, which covers only time spent inside
     * the VM and so excludes input delay, queueing, and the write path. The
     * same caught-up-versus-backlog caveat as `record_input_delay` applies. For
     * a transform with several output topics this records one sample per output
     * topic, since each is written and committed independently.
     */
    void record_e2e_latency(model::timestamp source_batch_timestamp);

private:
    friend class ProcessorTestFixture;

    uint64_t _read_bytes = 0;
    std::vector<uint64_t> _write_bytes;
    uint64_t _failures = 0;
    std::vector<uint64_t> _lag;
    absl::flat_hash_map<model::transform_report::processor::state, uint64_t>
      _processor_state;
    hist_t _input_delay;
    hist_t _e2e_latency;
};

} // namespace transform
