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

#include "model/record.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "ssx/semaphore.h"
#include "transform/io.h"
#include "wasm/engine.h"
#include "wasm/transform_probe.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>

#include <chrono>
#include <optional>

namespace transform::testing {

constexpr model::transform_id my_transform_id{42};
// NOLINTBEGIN(cert-err58-cpp)
static const model::ntp my_ntp = model::random_ntp();
static const model::transform_metadata my_single_output_metadata{
  .name = model::transform_name("xform"),
  .input_topic = model::topic_namespace(my_ntp.ns, my_ntp.tp.topic),
  .output_topics = {model::random_topic_namespace()},
  .environment = {{"FOO", "bar"}},
  .uuid = uuid_t::create(),
  .source_ptr = model::offset(9)};
static const model::transform_metadata my_multiple_output_metadata{
  .name = model::transform_name("xform-multi-output"),
  .input_topic = model::topic_namespace(my_ntp.ns, my_ntp.tp.topic),
  .output_topics = {
    model::random_topic_namespace(),
    model::random_topic_namespace(), 
    model::random_topic_namespace(),
  },
  .environment = {{"FOO", "bar"}},
  .uuid = uuid_t::create(),
  .source_ptr = model::offset(10)};
// NOLINTEND(cert-err58-cpp)

class fake_wasm_engine : public wasm::engine {
public:
    ss::future<> transform(
      model::record_batch batch,
      wasm::transform_probe*,
      wasm::transform_callback) override;

    void set_output_topics(std::vector<model::topic> topics);
    void set_use_default_output_topic();

    /**
     * Make the next `n` calls to transform() throw, simulating guest code
     * (or the engine itself) repeatedly failing - for exercising
     * transform_failure_policy. Calls past the nth succeed normally again.
     */
    void set_failures_remaining(uint32_t n);

    ss::future<> start() override;
    ss::future<> stop() override;

private:
    bool _started = false;
    std::optional<std::vector<model::topic>> _output_topics;
    uint32_t _failures_remaining = 0;
};

class fake_source : public source {
    static constexpr size_t max_queue_size = 64;

public:
    explicit fake_source() = default;

    ss::future<> start() override;
    ss::future<> stop() override;
    kafka::offset latest_offset() override;
    ss::future<std::optional<kafka::offset>>
    offset_at_timestamp(model::timestamp, ss::abort_source*) override;
    kafka::offset start_offset() const override;
    ss::future<model::record_batch_reader>
    read_batch(kafka::offset offset, ss::abort_source* as) override;
    ss::future<> wait_for_offset(
      kafka::offset offset,
      model::timeout_clock::time_point deadline,
      ss::abort_source* as) override;

    ss::future<> push_batch(model::record_batch batch);

    /**
     * Report having caught up, by returning an empty read when there is nothing
     * at or past the requested offset, instead of waiting for a batch to be
     * pushed.
     *
     * This mirrors the real `partition_source`, which short circuits to an
     * empty reader once it has caught up to the end of the log. The default
     * blocking behaviour hides `processor::run_consumer_loop`'s wait for new
     * data completely: `read_batch` never returns empty, so the processor
     * never has to wait, and no test can observe how long the processor
     * actually takes to pick up a newly appended record.
     */
    void set_empty_reads_when_caught_up(bool);

private:
    absl::btree_map<kafka::offset, model::record_batch> _batches;
    ss::condition_variable _cond_var;
    bool _empty_reads_when_caught_up = false;
};

class fake_sink : public sink {
public:
    ss::future<> write(ss::chunked_fifo<model::record_batch> batches) override;

    /**
     * Read one record, waiting up to `timeout` for one to arrive.
     *
     * The timeout is a parameter because tests that exercise the processor's
     * polling interval have to wait longer than a record normally takes.
     */
    ss::future<model::record>
    read(std::chrono::milliseconds timeout = std::chrono::seconds(1));
    bool empty() const { return _records.empty(); }

    /**
     * Pause writes for this sink. All calls to `write` will not resolve until
     * `uncork` is called.
     */
    void cork();

    /**
     * Unpause a sink that was paused via `cork`.
     */
    void uncork();

    /**
     * Make every subsequent call to `write` throw, mimicking the real
     * rpc_client_sink failing to produce (e.g. "Current node is not a leader
     * for partition").
     */
    void fail_writes();

    /**
     * Stop failing writes, mimicking the transient produce failure (e.g. a
     * leadership transfer) resolving so the sink can make progress again.
     */
    void resume_writes();

private:
    ss::chunked_fifo<model::record> _records;
    ss::condition_variable _cond_var;
    ssx::semaphore _cork = {ssx::semaphore::max_counter(), "fake_sink"};
    bool _fail = false;
};

class fake_offset_tracker : public offset_tracker {
public:
    ss::future<> start() override;
    ss::future<> stop() override;

    ss::future<absl::flat_hash_map<model::output_topic_index, kafka::offset>>
    load_committed_offsets() override;

    ss::future<>
      commit_offset(model::output_topic_index, kafka::offset) override;

    ss::future<>
      wait_for_committed_offset(model::output_topic_index, kafka::offset);

private:
    absl::flat_hash_map<model::output_topic_index, kafka::offset> _committed;
    ss::condition_variable _cond_var;
};

} // namespace transform::testing
