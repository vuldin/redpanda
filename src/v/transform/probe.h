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
    // A batch exhausted transform_failure_policy::max_retries and the
    // processor gave up on it (skipped it, or dead-lettered it first) -
    // distinct from increment_failure(), which fires on every individual
    // attempt: this is the rarer, worse outcome of a batch never
    // succeeding after every attempt this policy allowed.
    void increment_given_up();
    // A persisted guest-state snapshot existed on this partition but
    // couldn't be delivered into the guest's memory on start (see
    // processor::restore_guest_state) - the guest may now be running
    // without state it should have recovered. See
    // model::transform_state_options::require_state_recovery for what
    // happens next.
    void increment_state_recovery_failure();
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

    /**
     * Time the phases of bringing a processor up.
     *
     * Startup is what a transform pays every time its input partition changes
     * leader, not just on deploy, so together these say how long a transform
     * is dark across a leadership move and which phase is worth attacking.
     *
     * They are kept separate because the phases differ in whether they *could*
     * be done before leadership arrives: creating the processor, starting the
     * engine and restoring guest state touch nothing outside this process,
     * while loading committed offsets is only meaningful once this node
     * actually leads the partition. So `offset_load` is the floor on any
     * handover and the other three are what warming a target ahead of the move
     * could hide.
     */
    /**
     * How long this partition's transform was dark: from the broker beginning
     * to bring it up through to the processor reporting `running`.
     *
     * This is the headline number for a leadership move, so the clock has to
     * start where the expensive work does. It is therefore started by the
     * MANAGER, before the processor is created, and handed to
     * processor::start - fetching and compiling the module happens on the
     * manager's side of that boundary, and on a broker not already running
     * the transform it dominates every other phase (measured 2026-09-07:
     * ~3.3s of create against ~1ms for everything after it). Timing from
     * processor::start alone would report the cheap tail. Restarting a
     * processor that already exists has no create to account for, so there the
     * processor starts the clock itself.
     *
     * Also not the sum of the phase histograms below: bring-up runs as an
     * asynchronous chain, so this covers the reactor scheduling gaps between
     * phases too. Those are what a broker taking on many partitions at once
     * pays, since compiling a module is serialised onto one shard, and no
     * per-phase timing can see them.
     *
     * A bring-up abandoned before reaching `running` records nothing - see
     * processor::stop and the failed-create path in manager::create_processor
     * - so it cannot inflate this.
     */
    std::unique_ptr<hist_t::measurement> startup_measurement() {
        return _startup_latency.auto_measure();
    }
    std::unique_ptr<hist_t::measurement> processor_create_measurement() {
        return _processor_create_latency.auto_measure();
    }
    std::unique_ptr<hist_t::measurement> engine_start_measurement() {
        return _engine_start_latency.auto_measure();
    }
    std::unique_ptr<hist_t::measurement> state_restore_measurement() {
        return _state_restore_latency.auto_measure();
    }
    std::unique_ptr<hist_t::measurement> offset_load_measurement() {
        return _offset_load_latency.auto_measure();
    }

private:
    friend class ProcessorTestFixture;

    uint64_t _read_bytes = 0;
    std::vector<uint64_t> _write_bytes;
    uint64_t _failures = 0;
    uint64_t _given_up = 0;
    uint64_t _state_recovery_failures = 0;
    std::vector<uint64_t> _lag;
    absl::flat_hash_map<model::transform_report::processor::state, uint64_t>
      _processor_state;
    hist_t _input_delay;
    hist_t _e2e_latency;
    hist_t _startup_latency;
    hist_t _processor_create_latency;
    hist_t _engine_start_latency;
    hist_t _state_restore_latency;
    hist_t _offset_load_latency;
};

} // namespace transform
