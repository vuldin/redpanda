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
#include "base/seastarx.h"
#include "io.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "relay/fwd.h"
#include "transform/memory_limiter.h"
#include "transform/probe.h"
#include "transform/transfer_queue.h"
#include "utils/prefix_logger.h"
#include "wasm/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>
#include <optional>
#include <variant>

namespace transform {

// The limits to various buffers for this shard within the transform
// subsystem. We have two main "buffers" we need to limit, the amount of
// data we have ingress to the Wasm VM, and the amount of data we have
// egress from the Wasm VM. We split statically the memory available between
// these buffers so that there are starvation issues where we have to write
// more data in order to get data out of our write buffers and the read
// buffers have taken up all the memory.
//
// In addition to these semaphores, we also reserve 10% of this subsystem's
// memory as flex space.
//
// All of these memory limits have tunable overrides in the case of a
// miscalculation causing memory pressure on shards.
struct memory_limits {
    struct config {
        size_t read;
        size_t write;
    };
    explicit memory_limits(config cfg)
      : read_buffer_semaphore(cfg.read)
      , write_buffer_semaphore(cfg.write) {}
    memory_limiter read_buffer_semaphore;
    memory_limiter write_buffer_semaphore;
};

/**
 * Marks that the input has been processed up to (and including) `offset`.
 *
 * `source_timestamp` is the max timestamp of the source batch this marker
 * follows, carried through the pipeline so that the producer can report
 * end-to-end latency once the corresponding output has actually been written.
 * It is nullopt for the priming marker pushed when a producer starts, which
 * does not follow any source batch.
 */
struct progress_marker {
    kafka::offset offset;
    std::optional<model::timestamp> source_timestamp;
};

/**
 * A single piece of transformed output, plus the guest-chosen output
 * partition key, if any - nullopt means the default
 * same-index-as-input-partition routing.
 */
struct keyed_transformed_data {
    std::optional<iobuf> partition_key;
    model::transformed_data data;
};

/**
 * A holder of the result of a transform, which is either a batch of data or a
 * marker recording progress through the input.
 */
struct transformed_output {
    std::variant<keyed_transformed_data, progress_marker> data;

    // How much memory this object is using.
    size_t memory_usage() const;
};

/**
 * A processor is the driver of a transform for a single partition.
 *
 * At it's heart it's a fiber that reads->transforms->writes batches
 * from an input ntp to an output ntp.
 */
class processor {
public:
    using state = model::transform_report::processor::state;
    using state_callback
      = ss::noncopyable_function<void(model::transform_id, model::ntp, state)>;

    processor(
      model::transform_id,
      model::ntp,
      model::transform_metadata,
      ss::shared_ptr<wasm::engine>,
      state_callback,
      std::unique_ptr<source>,
      std::vector<std::unique_ptr<sink>>,
      std::unique_ptr<offset_tracker>,
      probe*,
      memory_limits*,
      std::unique_ptr<sink> dead_letter_sink = nullptr,
      std::unique_ptr<state_store> state_store = nullptr,
      relay::service* relay = nullptr);
    processor(const processor&) = delete;
    processor(processor&&) = delete;
    processor& operator=(const processor&) = delete;
    processor& operator=(processor&&) = delete;
    virtual ~processor() = default;
    /**
     * Bring the processor up.
     *
     * `bringup` is a startup clock the caller already started, for the case
     * where work that counts towards startup happened before this processor
     * existed - the manager starts one before creating the processor, because
     * fetching and compiling the module happens there. Pass nullptr when
     * there was no such work, e.g. restarting a processor that already exists,
     * and the clock is started here instead.
     *
     * No default argument on purpose: a default on a virtual is resolved from
     * the static type, so a caller working through a base reference could
     * silently get a different one than an override intended.
     */
    virtual ss::future<>
    start(std::unique_ptr<probe::hist_t::measurement> bringup);
    virtual ss::future<> stop();

    /**
     * Quiesce this processor before it loses its partition, so that work
     * already read is finished rather than discarded.
     *
     * stop() aborts everything and clears the pipeline, so anything read but
     * not yet committed is dropped and reprocessed by the next owner. That is
     * correct but it makes every planned leadership move emit duplicates, and
     * duplicates are not suppressible across owners because a processor takes
     * a fresh producer_id on every start. drain() instead stops only the
     * consumer, lets what is already in flight finish and commit, flushes the
     * commit batcher, and takes a final state checkpoint so the guest's state
     * and its committed offset agree at one point.
     *
     * Bounded by `deadline` and best-effort: returns false if the pipeline
     * did not quiesce in time, having left the processor in a state where a
     * normal stop() is still the correct next call. A caller that cannot wait
     * should stop() without draining rather than skip the stop.
     *
     * SINGLE-SHOT per processor lifetime. The second and later calls return
     * the first call's outcome without waiting again, because leadership is
     * relinquished through two hooks that both fire on the transfer path -
     * cluster::partition's, before the STM prepare phase, and raft's, after -
     * and only the first runs while produces still flow. Re-waiting in the
     * second call puts the whole budget back on the produce path: measured
     * locally, a 5000ms budget cost 5.17s of produce max on the transform's
     * own input partition while an untransformed control partition on the
     * same reactor stayed at 194ms. One bounded chance is the semantic; a
     * second identical wait can only add cost.
     *
     * This does NOT stop the processor - stop() must still be called.
     */
    virtual ss::future<bool> drain(model::timeout_clock::time_point deadline);

    bool is_running() const;
    model::transform_id id() const;
    const model::ntp& ntp() const;
    const model::transform_metadata& meta() const;
    int64_t current_lag() const;

private:
    ss::future<> run_consumer_loop(kafka::offset);
    ss::future<> run_transform_loop();
    ss::future<> run_all_producers(
      absl::flat_hash_map<model::output_topic_index, kafka::offset>);
    ss::future<> run_producer_loop(
      model::output_topic_index,
      model::topic,
      transfer_queue<transformed_output>*,
      sink*,
      kafka::offset,
      ss::abort_source&);
    ss::future<absl::flat_hash_map<model::output_topic_index, kafka::offset>>
    load_latest_committed();
    void report_lag(model::output_topic_index, int64_t);

    // Restores a persisted guest-state snapshot (if any) into the guest's
    // registered shared-memory region before the transform/consumer loops
    // start, so the guest resumes from its own last checkpoint instead of
    // silently starting from zeroed memory. Throws - a hard start()
    // failure, going through the same restart/backoff path as any other -
    // if _meta.state_options.require_state_recovery is set and a
    // persisted snapshot exists but can't be delivered into the guest's
    // memory. See model::transform_state_options for why that's opt-in.
    ss::future<> restore_guest_state();
    // Best-effort periodic checkpoint of the guest's own state, called
    // after every successful transform() call. A no-op unless the engine
    // actually has bytes to offer (i.e. the binary was granted the
    // shared_memory capability and the guest has registered a region) -
    // see wasm::engine::read_shared_memory. Paced by checkpoint_interval
    // rather than every batch: this is a coarse, occasional durability
    // improvement, not something that should add a raft round trip to
    // this transform's hot path.
    // `force` skips the interval check, for a drain that needs the guest's
    // state written at the same point its offsets were committed rather than
    // whenever the periodic timer next comes round.
    //
    // `through` is the last input offset the guest has applied - the copy is
    // taken between invocations, so the state is exactly that - and is
    // persisted with it (state_envelope.h). The checkpoint is only written
    // once every output has written its records through `through`: a state
    // ahead of its outputs would, after a crash, resume past outputs that
    // were never written (and that a precommit relay consumer may have seen).
    ss::future<> maybe_checkpoint_state(kafka::offset through, bool force = false);
    // Resolves once every output has written (or skipped as already durable)
    // its records through `offset`, or on stop.
    ss::future<> wait_written_through(kafka::offset offset);

    template<typename... Future>
    ss::future<> when_all_shutdown(Future&&...);
    ss::future<> handle_processor_task(ss::future<>);

    model::transform_id _id;
    model::ntp _ntp;
    model::transform_metadata _meta;
    ss::shared_ptr<wasm::engine> _engine;
    std::unique_ptr<source> _source;
    std::unique_ptr<offset_tracker> _offset_tracker;
    state_callback _state_callback;
    probe* _probe;
    // Aborts the consumer stage ALONE, so a drain can stop reading new
    // records while the transform and producer stages keep running to finish
    // what is already in flight. stop() aborts this too - it must, or the
    // consumer would outlive an ordinary stop.
    ss::abort_source _consumer_as;
    // Highest input offset handed to the pipeline. A drain is complete when
    // every output has committed at least this far.
    kafka::offset _last_read_offset = kafka::offset::min();
    // Set by the first drain() of this processor's lifetime, cleared by
    // start(). Holds that call's result so a second drain can answer without
    // repeating the wait - see drain()'s comment on why that matters.
    std::optional<bool> _drain_outcome;
    // Alive from the start of start() until `running` is reported, so it spans
    // the whole asynchronous startup chain. Cancelled rather than recorded if
    // we are stopped first (see stop()), because a start that never finished
    // is not a startup duration.
    std::unique_ptr<probe::hist_t::measurement> _startup_m;

    static constexpr size_t buffer_chunk_size = 8;

    transfer_queue<model::record_batch, buffer_chunk_size>
      _consumer_transform_pipe;

    struct output {
        model::output_topic_index index;
        transfer_queue<transformed_output> queue;
        std::unique_ptr<sink> sink;
    };
    absl::flat_hash_map<model::topic, output> _outputs;
    output* _default_output = nullptr;

    // Only engaged if _meta.failure_policy.dead_letter_topic is set (see
    // transform/api.cc's create_processor). Written directly, inline, from
    // run_transform_loop's failure-policy branch - unlike _outputs, which
    // goes through its own transfer_queue/producer-loop pair for ordinary
    // backpressure-aware output, this is the rare, exceptional path and
    // doesn't need that machinery.
    std::unique_ptr<sink> _dead_letter_sink;
    // Consecutive failures processing the batch currently at the front of
    // _consumer_transform_pipe. Resets to 0 on any success, or once a
    // batch is given up on and the loop moves to the next one. Survives a
    // transform_manager-driven restart because the manager restarts by
    // calling start() again on this same processor object, not by
    // constructing a new one (see transform_manager.cc's start_processor).
    uint32_t _consecutive_transform_failures{0};

    // Only engaged when transform::transform_state_stm is reachable for
    // this ntp (see transform/api.cc's create_processor) - null is a
    // legitimate "state persistence unavailable right now" state, not an
    // error; restore_guest_state/maybe_checkpoint_state both treat it as
    // a no-op.
    std::unique_ptr<state_store> _state_store;

    // The shard-local relay this processor fans its transformed output out
    // to, in addition to the durable Kafka write. Null when the relay is
    // disabled - the callback then skips the push entirely, so transforms
    // pay nothing for a relay they aren't using. Owned by the application,
    // outlives this processor.
    relay::service* _relay;
    // RELAY_DELIVERY=committed (relay_source_env.h): relay consumers get a
    // record only after its output write is quorum-acknowledged, pushed from
    // the producer loop instead of from the transform callback.
    bool _relay_after_commit;
    ss::lowres_clock::time_point _last_checkpoint_at
      = ss::lowres_clock::time_point::min();
    // The periodic checkpoint replicating in the background, if any (see
    // maybe_checkpoint_state). At most one is in flight; a forced
    // checkpoint and stop() wait for it.
    ss::future<> _checkpoint_in_flight = ss::now();
    // The input offset the restored guest state reflects (state_envelope.h);
    // std::nullopt with no snapshot, or one from before the envelope. When
    // set, the input resumes right after it instead of at the committed
    // output offsets, so the state is replayed forward exactly.
    std::optional<kafka::offset> _restored_input_offset;
    // Last input offset the guest applied, for a drain's checkpoint.
    kafka::offset _last_transformed_offset = kafka::offset::min();
    // The input offset of the batch being transformed right now: the relay
    // callback skips records already delivered before a restart.
    kafka::offset _current_input_offset = kafka::offset::min();
    // Input records at or below this were delivered before this lifetime:
    // their outputs are durable (committed) on every output, so a replay of
    // them from the restored state must not push them to relay consumers
    // again.
    kafka::offset _relay_replay_through = kafka::offset::min();
    // Per output, the input offset its producer has written (or skipped as
    // already durable) through; signalled on every advance.
    absl::flat_hash_map<model::output_topic_index, kafka::offset>
      _written_through;
    ss::condition_variable _written_cv;

    ss::abort_source _as;
    ss::future<> _task;
    prefix_logger _logger;

    std::vector<int64_t> _last_reported_lag;
};
} // namespace transform
