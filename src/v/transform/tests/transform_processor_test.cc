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

#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "test_utils/async.h"
#include "test_utils/random_bytes.h"
#include "transform/tests/test_fixture.h"
#include "transform/transform_processor.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <iterator>
#include <memory>
#include <vector>

namespace transform {

namespace {

MATCHER(SameRecordEq, "") {
    const model::record& a = std::get<0>(arg);
    const model::record& b = std::get<1>(arg).get();
    if (a.key() != b.key()) {
        *result_listener << "expected same key: " << a.key() << " vs "
                         << b.key();
        return false;
    }
    if (a.value() != b.value()) {
        *result_listener << "expected same value: " << a.value() << " vs "
                         << b.value();
        return false;
    }
    if (a.headers() != b.headers()) {
        *result_listener << "expected same headers: " << a.headers() << " vs "
                         << b.headers();
        return false;
    }
    return true;
}

// A helper to ensure all records have the same key/value/headers (but doesn't
// check other metadata).
auto SameRecords(std::span<model::record> expected) {
    std::vector<std::reference_wrapper<const model::record>> expected_refs;
    expected_refs.reserve(expected.size());
    for (const auto& r : expected) {
        expected_refs.push_back(std::ref(r));
    }
    return ::testing::Pointwise(SameRecordEq(), expected_refs);
}

struct stats_snapshot {
    uint64_t read_bytes;
    std::vector<uint64_t> write_bytes;
    std::vector<uint64_t> lag;
};

struct fixture_param {
    model::transform_metadata meta;
    bool autostart = true;
    // Wires a testing::fake_state_store into the processor, standing in
    // for transform::transform_state_stm - see the guest-state-recovery
    // tests (PR-16).
    bool with_state_store = false;
};

model::transform_metadata with_max_retries(uint32_t max_retries) {
    auto meta = testing::my_single_output_metadata;
    meta.failure_policy.max_retries = max_retries;
    return meta;
}

model::transform_metadata with_dead_letter(uint32_t max_retries) {
    auto meta = with_max_retries(max_retries);
    meta.failure_policy.dead_letter_topic = model::random_topic_namespace();
    return meta;
}

model::transform_metadata with_required_state_recovery() {
    auto meta = testing::my_single_output_metadata;
    meta.state_options.require_state_recovery = true;
    return meta;
}

} // namespace

class ProcessorTestFixture : public ::testing::TestWithParam<fixture_param> {
public:
    void SetUp() override {
        auto engine = ss::make_shared<testing::fake_wasm_engine>();
        _engine = engine.get();
        auto src = std::make_unique<testing::fake_source>();
        _src = src.get();
        std::vector<std::unique_ptr<transform::sink>> sinks;
        const fixture_param& param = GetParam();
        for (size_t i = 0; i < param.meta.output_topics.size(); ++i) {
            auto sink = std::make_unique<testing::fake_sink>();
            _sinks.push_back(sink.get());
            sinks.push_back(std::move(sink));
        }
        auto offset_tracker = std::make_unique<testing::fake_offset_tracker>();
        _offset_tracker = offset_tracker.get();
        std::unique_ptr<transform::sink> dead_letter_sink;
        if (param.meta.failure_policy.dead_letter_topic) {
            auto dl_sink = std::make_unique<testing::fake_sink>();
            _dead_letter_sink = dl_sink.get();
            dead_letter_sink = std::move(dl_sink);
        }
        std::unique_ptr<transform::state_store> state_store;
        if (param.with_state_store) {
            auto ss_impl = std::make_unique<testing::fake_state_store>();
            _state_store = ss_impl.get();
            state_store = std::move(ss_impl);
        }
        _probe.setup_metrics(param.meta);
        _p = std::make_unique<transform::processor>(
          testing::my_transform_id,
          testing::my_ntp,
          param.meta,
          std::move(engine),
          [this](auto, auto, processor::state state) {
              if (state == processor::state::errored) {
                  ++_error_count;
              }
          },
          std::move(src),
          std::move(sinks),
          std::move(offset_tracker),
          &_probe,
          &_memory_limits,
          std::move(dead_letter_sink),
          std::move(state_store));
        if (param.autostart) {
            _p->start().get();
            // Wait for the initial offset to be committed so we know that the
            // processor is actually ready, otherwise it could be possible
            // that the processor picks up after the initial records are added
            // to the partition.
            wait_for_committed_offset(kafka::offset{});
        }
    }
    void TearDown() override { _p->stop().get(); }

    bool wait_for_committed_offset(model::output_topic_index idx) {
        return wait_for_committed_offset(idx, kafka::prev_offset(_offset));
    }
    bool wait_for_committed_offset(model::offset o) {
        return wait_for_committed_offset(model::offset_cast(o));
    }
    bool wait_for_committed_offset(kafka::offset o) {
        for (auto idx : output_topics()) {
            if (!wait_for_committed_offset(idx, o)) {
                return false;
            }
        }
        return true;
    }
    bool
    wait_for_committed_offset(model::output_topic_index i, model::offset o) {
        return wait_for_committed_offset(i, model::offset_cast(o));
    }
    bool
    wait_for_committed_offset(model::output_topic_index i, kafka::offset o) {
        try {
            _offset_tracker->wait_for_committed_offset(i, o).get();
            return true;
        } catch (const ss::condition_variable_timed_out&) {
            return false;
        }
    }
    bool wait_for_all_committed() {
        return wait_for_committed_offset(kafka::prev_offset(_offset));
    }
    auto committed_offsets() {
        return _offset_tracker->load_committed_offsets().get();
    }

    void set_default_output() { _engine->set_use_default_output_topic(); }
    void set_devnull_output() { _engine->set_output_topics({}); }
    void set_tee_output() {
        std::vector<model::topic> topics;
        for (const auto& tp_ns : GetParam().meta.output_topics) {
            topics.push_back(tp_ns.tp);
        }
        _engine->set_output_topics(std::move(topics));
    }

    std::vector<model::record> make_records(size_t n) {
        std::vector<model::record> records;
        std::generate_n(std::back_inserter(records), n, [&records] {
            return model::test::make_random_record(
              int(records.size()), tests::random_iobuf());
        });
        return records;
    }

    // Push a batch into the source returning the current max offset of the
    // source after the push.
    kafka::offset push_batch(const std::vector<model::record>& records) {
        ss::chunked_fifo<model::transformed_data> data;
        for (const auto& r : records) {
            data.push_back(model::transformed_data::from_record(r.copy()));
        }
        auto batch = model::transformed_data::make_batch(
          _fixed_time, std::move(data));
        _fixed_time = model::timestamp(
          _fixed_time.value() + batch.record_count());
        batch.header().base_offset = kafka::offset_cast(_offset);
        _offset += batch.record_count();
        _src->push_batch(std::move(batch)).get();
        return kafka::prev_offset(_offset);
    }

    // Push a batch of size 1, returns the new max offset.
    kafka::offset push_record(const model::record& record) {
        std::vector<model::record> batch;
        batch.push_back(record.copy());
        return push_batch(batch);
    }

    std::vector<model::record>
    read_records(model::output_topic_index idx, size_t n) {
        std::vector<model::record> records;
        for (size_t i = 0; i < n; ++i) {
            records.push_back(_sinks[idx()]->read().get());
        }
        return records;
    }
    std::vector<model::record> read_records(size_t n) {
        return read_records({}, n);
    }
    std::vector<model::record> read_records_within(
      model::output_topic_index idx,
      size_t n,
      std::chrono::milliseconds timeout) {
        std::vector<model::record> records;
        for (size_t i = 0; i < n; ++i) {
            records.push_back(_sinks[idx()]->read(timeout).get());
        }
        return records;
    }

    // Make the source report having caught up instead of blocking until data
    // arrives, the way the real partition_source does.
    void report_caught_up_reads() {
        _src->set_empty_reads_when_caught_up(true);
    }
    bool sink_empty(model::output_topic_index idx) {
        return _sinks[idx()]->empty();
    }
    uint64_t error_count() const { return _error_count; }
    uint64_t given_up_count() const { return _probe._given_up; }
    int64_t lag() const { return _p->current_lag(); }

    void fail_next_transforms(uint32_t n) {
        _engine->set_failures_remaining(n);
    }
    model::record read_dead_letter(
      std::chrono::milliseconds timeout = std::chrono::seconds(1)) {
        return _dead_letter_sink->read(timeout).get();
    }
    bool dead_letter_empty() const {
        return !_dead_letter_sink || _dead_letter_sink->empty();
    }

    void set_shared_memory_registered(bool registered) {
        _engine->set_shared_memory_region_registered(registered);
    }
    bool write_shared_memory(const iobuf& data) {
        return _engine->write_shared_memory(iobuf_to_bytes(data));
    }
    std::optional<iobuf> current_shared_memory() {
        return _engine->read_shared_memory();
    }
    void prime_persisted_state(iobuf state) {
        _state_store->prime_state(std::move(state));
    }
    uint64_t state_store_save_count() const {
        return _state_store ? _state_store->save_count() : 0;
    }
    uint64_t state_recovery_failure_count() const {
        return _probe._state_recovery_failures;
    }

    void cork_sink(model::output_topic_index idx) { _sinks[idx()]->cork(); }
    void uncork_sink(model::output_topic_index idx) { _sinks[idx()]->uncork(); }
    void fail_sink(model::output_topic_index idx) {
        _sinks[idx()]->fail_writes();
    }
    void recover_sink(model::output_topic_index idx) {
        _sinks[idx()]->resume_writes();
    }
    bool processor_running() const { return _p->is_running(); }

    std::vector<model::output_topic_index> output_topics() const {
        std::vector<model::output_topic_index> indexes;
        size_t size = GetParam().meta.output_topics.size();
        indexes.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            indexes.emplace_back(i);
        }
        return indexes;
    }

    void restart() {
        stop();
        start();
    }
    void stop() { _p->stop().get(); }
    void start() { _p->start().get(); }

    ss::future<> initiate_stop() { return _p->stop(); }

    stats_snapshot current_stats() {
        return {
          .read_bytes = _probe._read_bytes,
          .write_bytes = _probe._write_bytes,
          .lag = _probe._lag,
        };
    }

private:
    static constexpr kafka::offset start_offset = kafka::offset(0);

    memory_limits _memory_limits = memory_limits(
      memory_limits::config{.read = 10_MiB, .write = 10_MiB});
    kafka::offset _offset = start_offset;
    model::timestamp _fixed_time = model::timestamp::min();
    std::unique_ptr<transform::processor> _p;
    testing::fake_wasm_engine* _engine = nullptr;
    testing::fake_source* _src = nullptr;
    testing::fake_offset_tracker* _offset_tracker = nullptr;
    std::vector<testing::fake_sink*> _sinks;
    testing::fake_sink* _dead_letter_sink = nullptr;
    testing::fake_state_store* _state_store = nullptr;
    uint64_t _error_count = 0;
    probe _probe;
};

TEST_P(ProcessorTestFixture, HandlesDoubleStops) {
    stop();
    stop();
}

TEST_P(ProcessorTestFixture, HandlesDoubleStarts) { start(); }

using ::testing::Contains;
using ::testing::Gt;

TEST_P(ProcessorTestFixture, ProcessOne) {
    auto batch = make_records(1);
    push_batch(batch);
    auto returned = read_records(1);
    EXPECT_THAT(returned, SameRecords(batch));
    auto stats = current_stats();
    EXPECT_GT(stats.read_bytes, 0);
    EXPECT_THAT(stats.write_bytes, Contains(Gt(0)));
    EXPECT_EQ(stats.write_bytes.size(), GetParam().meta.output_topics.size());
    EXPECT_EQ(error_count(), 0);
}

TEST_P(ProcessorTestFixture, ProcessMany) {
    constexpr size_t n = 32;
    auto batch = make_records(n);
    push_batch(batch);
    auto returned = read_records(n);
    EXPECT_THAT(returned, SameRecords(batch));
    EXPECT_EQ(error_count(), 0);
}

TEST_P(ProcessorTestFixture, TracksOffsets) {
    constexpr int num_records = 32;
    auto first_batches = make_records(num_records);
    auto second_batches = make_records(num_records);
    for (auto& b : first_batches) {
        push_record(b.share());
    }
    auto returned = read_records(num_records);
    EXPECT_THAT(returned, SameRecords(first_batches)) << "first batch mismatch";
    // If we don't wait for the last commit to happen, it's possible that
    // we restart and get duplicates.
    ASSERT_TRUE(wait_for_all_committed());
    restart();
    for (auto& b : second_batches) {
        push_record(b.share());
    }
    returned = read_records(num_records);
    EXPECT_THAT(returned, SameRecords(second_batches))
      << "second batch mismatch";
    EXPECT_EQ(error_count(), 0);
}

TEST_P(ProcessorTestFixture, HandlesEmptyBatches) {
    auto batch_one = make_records(1);
    push_batch(batch_one);
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_one));

    auto batch_two = make_records(1);
    set_devnull_output();
    push_batch(batch_two);
    // We never will read batch two, it was filtered out
    // but we should still get a commit for batch two
    ASSERT_TRUE(wait_for_all_committed());

    auto batch_three = make_records(1);
    set_default_output();
    push_batch(batch_three);
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_three));
}

TEST_P(ProcessorTestFixture, LagOffByOne) {
    EXPECT_EQ(lag(), 0);
    auto batch_one = make_records(1);
    push_batch(batch_one);
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_one));
    // With multiple output topics, we need to ensure all outputs have reported
    // their lag.
    tests::drain_task_queue().get();
    EXPECT_EQ(lag(), 0);
}

TEST_P(ProcessorTestFixture, LagOverflowBug) {
    stop();
    auto batch_one = make_records(1);
    push_batch(batch_one);
    start();
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_one));
    // With multiple output topics, we need to ensure all outputs have reported
    // their lag.
    tests::drain_task_queue().get();
    EXPECT_EQ(lag(), 0);
}

// Regression test for the "stuck transform" bug. When one output's producer
// fails to produce (e.g. a transient "not a leader for partition"), the failure
// must be reported as state::errored so the manager restarts the processor,
// regardless of how many outputs the transform has.
//
// The bug: with MULTIPLE outputs, `run_all_producers()` fanned out via
// `ss::parallel_for_each()`, which captures the first exception but waits for
// every loop to finish before resolving. Nothing aborted the shared abort
// source, so the surviving producer loop(s) spin forever, pinning
// parallel_for_each pending: state::errored never fired, the processor stayed
// "running", and it made no progress until an external stop (== an `rpk
// transform pause`/`resume`). Single-output transforms were unaffected because
// parallel_for_each over one element resolves exceptionally right away.
//
// The fix drives the producer loops off a composite abort source, so the first
// failing producer unwinds its siblings (without touching the processor's own
// abort source) and propagates exactly one error.
TEST_P(ProcessorTestFixture, ProduceFailureIsReportedForAnyOutputCount) {
    set_tee_output();

    // Healthy baseline so the pipeline is flowing and committed.
    auto baseline = make_records(1);
    push_batch(baseline);
    for (auto o : output_topics()) {
        EXPECT_THAT(read_records(o, 1), SameRecords(baseline));
    }
    ASSERT_TRUE(wait_for_all_committed());

    // Fail only output 0; any other outputs stay healthy.
    fail_sink(model::output_topic_index(0));
    push_batch(make_records(1));
    tests::drain_task_queue().get();

    // The producers all unwind instead of wedging. The processor still reports
    // running (its abort source is untouched); like a single-output failure, it
    // relies on the manager observing the error and restarting it.
    EXPECT_TRUE(processor_running());
    // Output 0's producer died, so nothing is ever written there.
    EXPECT_TRUE(sink_empty(model::output_topic_index(0)));
    // The failure is reported exactly once, no matter the output count: the
    // consumer/transform loops keep running on the untouched abort source, so
    // the manager restarts the processor without spurious duplicate errors.
    EXPECT_EQ(error_count(), 1u)
      << "produce failure should be reported as errored exactly once";
}

// A produce failure must leave the processor cleanly restartable: the manager's
// recovery path stops and then restarts the same processor instance, so stop()
// has to tear down (and the wedge fix must not have aborted the processor's own
// abort source, which would short-circuit stop() and leave the engine started
// for the restart to double-start). Exercise that full cycle and assert the
// restart is clean and the processor resumes producing.
TEST_P(ProcessorTestFixture, RecoversFromProduceFailureViaRestart) {
    set_tee_output();

    // Healthy baseline so the pipeline is flowing and committed.
    auto baseline = make_records(1);
    push_batch(baseline);
    for (auto o : output_topics()) {
        EXPECT_THAT(read_records(o, 1), SameRecords(baseline));
    }
    ASSERT_TRUE(wait_for_all_committed());

    // A producer fails and the error is reported, exactly what the manager
    // observes before it restarts.
    fail_sink(model::output_topic_index(0));
    push_batch(make_records(1));
    tests::drain_task_queue().get();
    ASSERT_EQ(error_count(), 1u);

    // Recovery: the transient failure clears and the manager stops and restarts
    // the same processor instance.
    recover_sink(model::output_topic_index(0));
    restart();
    tests::drain_task_queue().get();

    // A clean restart produces no further errors.
    ASSERT_EQ(error_count(), 1u)
      << "restart after a produce failure should not report a new error";
    EXPECT_TRUE(processor_running());

    // The restarted processor resumes producing to every output.
    auto resumed = make_records(1);
    push_batch(resumed);
    for (auto o : output_topics()) {
        EXPECT_FALSE(read_records(o, 1).empty());
    }
}

// Measures how long a record appended to an idle transform waits before its
// output appears, and guards against regressing back to polling.
//
// Every other test in this file reads through a source that blocks until data
// arrives, so the processor is always handed a batch the moment one exists and
// this idle path is never exercised. The real partition_source instead returns
// an empty read once it has caught up, and previously that sent the processor
// to sleep for a jittered ~1-1.5s interval regardless of how soon the next
// record actually arrived - a record appended during that sleep simply waited
// it out. `report_caught_up_reads` reproduces the empty-read behavior so this
// path is exercised at all; `fake_source::wait_for_offset` mirrors the fix
// (notify on push instead of sleeping), so this test's bound is a real
// regression guard, not just an observation.
//
// The bound is generous relative to the sub-millisecond common case
// specifically to absorb CI scheduling noise without becoming flaky - it only
// needs to be tight enough to fail if this ever regresses back to a fixed
// polling interval.
TEST_P(ProcessorTestFixture, MeasuresIdlePollingDelay) {
    using namespace std::chrono_literals;
    constexpr auto generous_timeout = 30s;
    constexpr auto regression_guard_bound = 500ms;

    report_caught_up_reads();

    // Push one record through so the processor reaches its caught-up state and
    // starts waiting on the next one.
    auto warmup = make_records(1);
    push_batch(warmup);
    EXPECT_FALSE(read_records_within({}, 1, generous_timeout).empty());

    // Now append to an idle processor and time how long the round trip takes.
    auto batch = make_records(1);
    auto start = std::chrono::steady_clock::now();
    push_batch(batch);
    auto returned = read_records_within({}, 1, generous_timeout);
    auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

    EXPECT_THAT(returned, SameRecords(batch));
    EXPECT_EQ(error_count(), 0);
    EXPECT_LT(delay, regression_guard_bound)
      << "idle-path delay of " << delay.count()
      << "ms looks like a regression back to fixed-interval polling";
    GTEST_LOG_(INFO) << "idle-path delay: " << delay.count() << "ms";
}

INSTANTIATE_TEST_SUITE_P(
  GenericProcessorTest,
  ProcessorTestFixture,
  ::testing::Values(
    fixture_param{testing::my_single_output_metadata},
    fixture_param{testing::my_multiple_output_metadata}));

// Two separate aliases, each with its own single-value instantiation below
// (the same shape ProcessorTimequeryTestFixture already uses) - not one
// shared alias, since these two scenarios need different metadata
// (dead_letter_topic set or not) and asserting the wrong one against the
// other's param would be a real, not just cosmetic, mismatch.
using ProcessorSkipPolicyTestFixture = ProcessorTestFixture;
using ProcessorDeadLetterPolicyTestFixture = ProcessorTestFixture;

// max_retries=2 allows 3 total attempts (the original, plus 2 retries)
// before the processor gives up on a batch. The first 2 failures each go
// through a full stop/restart cycle, identical to any other transform
// failure today (RecoversFromProduceFailureViaRestart exercises that same
// cycle) - only the 3rd is new behavior.
TEST_P(ProcessorSkipPolicyTestFixture, GivesUpAfterMaxRetriesAndSkips) {
    fail_next_transforms(100);
    push_batch(make_records(1));
    for (uint64_t attempt = 1; attempt <= 2; ++attempt) {
        tests::drain_task_queue().get();
        ASSERT_EQ(error_count(), attempt)
          << "attempt " << attempt << " is still within max_retries, so it "
          << "should error out and get restarted exactly like any other "
             "transform failure";
        restart();
    }
    // The 3rd attempt exceeds max_retries=2 - the processor gives up on
    // this batch internally instead of erroring out a 3rd time.
    tests::drain_task_queue().get();
    EXPECT_EQ(error_count(), 2u)
      << "giving up must not report a new error - the manager never needs "
         "to restart the processor for this outcome";
    EXPECT_TRUE(processor_running());
    EXPECT_EQ(given_up_count(), 1u);
    EXPECT_TRUE(dead_letter_empty())
      << "no dead_letter_topic was configured, so the batch is just "
         "skipped, not preserved anywhere";

    // The poison batch is gone - a healthy batch now flows normally.
    fail_next_transforms(0);
    auto healthy = make_records(1);
    push_batch(healthy);
    EXPECT_THAT(read_records(1), SameRecords(healthy));
}

TEST_P(ProcessorDeadLetterPolicyTestFixture, GivesUpAndDeadLetters) {
    fail_next_transforms(100);
    auto poison = make_records(1);
    push_batch(poison);
    for (uint64_t attempt = 1; attempt <= 2; ++attempt) {
        tests::drain_task_queue().get();
        ASSERT_EQ(error_count(), attempt);
        restart();
    }
    tests::drain_task_queue().get();
    EXPECT_EQ(error_count(), 2u);
    EXPECT_TRUE(processor_running());
    EXPECT_EQ(given_up_count(), 1u);

    // The raw, untransformed poison batch was produced to the dead-letter
    // topic before the processor advanced past it.
    std::vector<model::record> dead_lettered;
    dead_lettered.push_back(read_dead_letter());
    EXPECT_THAT(dead_lettered, SameRecords(poison));

    fail_next_transforms(0);
    auto healthy = make_records(1);
    push_batch(healthy);
    EXPECT_THAT(read_records(1), SameRecords(healthy));
}

INSTANTIATE_TEST_SUITE_P(
  SkipPolicyProcessorTest,
  ProcessorSkipPolicyTestFixture,
  ::testing::Values(fixture_param{with_max_retries(2)}));

INSTANTIATE_TEST_SUITE_P(
  DeadLetterPolicyProcessorTest,
  ProcessorDeadLetterPolicyTestFixture,
  ::testing::Values(fixture_param{with_dead_letter(2)}));

using ProcessorStateRecoveryTestFixture = ProcessorTestFixture;
using ProcessorRequiredStateRecoveryTestFixture = ProcessorTestFixture;

TEST_P(
  ProcessorStateRecoveryTestFixture, RestoresCheckpointedStateAfterRestart) {
    set_shared_memory_registered(true);
    auto state = tests::random_iobuf();
    ASSERT_TRUE(write_shared_memory(state));

    // The very first successful batch triggers a checkpoint - see
    // processor's _last_checkpoint_at, which starts at time_point::min().
    push_batch(make_records(1));
    tests::drain_task_queue().get();
    ASSERT_EQ(state_store_save_count(), 1u);

    // Simulate a restart: the same transform_state_stm-backed store
    // persists across it, but the engine's own linear memory does not -
    // see fake_wasm_engine::start().
    restart();
    tests::drain_task_queue().get();

    auto restored = current_shared_memory();
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(*restored, state);
}

TEST_P(
  ProcessorRequiredStateRecoveryTestFixture,
  FailsLoudlyWhenRequiredStateCannotBeRestored) {
    // A snapshot exists from "before", but the engine's shared-memory
    // region is not registered this time (the fixture default) -
    // simulates the binary losing its capability grant, or the guest
    // simply not registering a region on this particular start - so the
    // persisted snapshot can't be delivered into guest memory.
    prime_persisted_state(tests::random_iobuf());

    start();
    tests::drain_task_queue().get();

    EXPECT_EQ(error_count(), 1u);
    EXPECT_EQ(state_recovery_failure_count(), 1u);
    // Matches every other processor error in this suite (e.g.
    // GivesUpAndDeadLetters above): an error alone doesn't mark the
    // processor as stopped - only transform_manager's restart loop (not
    // exercised by this fixture) or an explicit stop() does that.
    EXPECT_TRUE(processor_running());
}

INSTANTIATE_TEST_SUITE_P(
  StateRecoveryProcessorTest,
  ProcessorStateRecoveryTestFixture,
  ::testing::Values(
    fixture_param{.meta = testing::my_single_output_metadata,
                  .with_state_store = true}));

INSTANTIATE_TEST_SUITE_P(
  RequiredStateRecoveryProcessorTest,
  ProcessorRequiredStateRecoveryTestFixture,
  ::testing::Values(
    fixture_param{.meta = with_required_state_recovery(),
                  .autostart = false,
                  .with_state_store = true}));

// Alias the test name so that we can write specialized tests for multiple
// output topics.
using ProcessorTimequeryTestFixture = ProcessorTestFixture;

TEST_P(ProcessorTimequeryTestFixture, StartAtTime) {
    constexpr size_t n = 10;
    std::vector<model::record> records;
    for (size_t i = 0; i < n; ++i) {
        auto batch_one = make_records(1);
        push_batch(batch_one);
        records.push_back(batch_one.front().copy());
    }
    start();
    ASSERT_TRUE(wait_for_all_committed());
    // We should skip the first 4 records and start exactly at timestamp=4
    EXPECT_THAT(read_records(6), SameRecords(std::span(records).subspan(4)));
}

TEST_P(ProcessorTimequeryTestFixture, BatchGranularity) {
    std::vector<model::record> records;
    for (size_t i = 0; i < 3; ++i) {
        auto batch = make_records(3);
        push_batch(batch);
        for (const auto& r : batch) {
            records.push_back(r.copy());
        }
    }
    start();
    ASSERT_TRUE(wait_for_all_committed());
    // We don't split batches to start at the exact right time, so you can get
    // older records if they are batched together.
    EXPECT_THAT(read_records(3), SameRecords(std::span(records).subspan(6)));
}

INSTANTIATE_TEST_SUITE_P(
  TimequeryProcessorTest,
  ProcessorTimequeryTestFixture,
  ::testing::Values([]() {
      auto meta = testing::my_single_output_metadata;
      meta.offset_options.position = model::timestamp(4);
      return fixture_param{.meta = meta, .autostart = false};
  }()));

// Alias the test name so that we can write specialized tests for multiple
// output topics.
using MultipleOutputsProcessorTestFixture = ProcessorTestFixture;

using ::testing::Each;

TEST_P(MultipleOutputsProcessorTestFixture, ProcessOne) {
    set_tee_output();
    auto batch = make_records(1);
    push_batch(batch);
    for (auto output : output_topics()) {
        auto returned = read_records(output, 1);
        EXPECT_THAT(returned, SameRecords(batch));
    }
    auto stats = current_stats();
    EXPECT_GT(stats.read_bytes, 0);
    EXPECT_THAT(stats.write_bytes, Each(Gt(0)));
    EXPECT_EQ(stats.write_bytes.size(), GetParam().meta.output_topics.size());
    EXPECT_EQ(error_count(), 0);
}

TEST_P(MultipleOutputsProcessorTestFixture, ProcessMany) {
    constexpr size_t n = 32;
    set_tee_output();
    auto batch = make_records(n);
    push_batch(batch);
    for (auto output : output_topics()) {
        auto returned = read_records(output, n);
        EXPECT_THAT(returned, SameRecords(batch));
    }
    EXPECT_EQ(error_count(), 0);
}

using ::testing::Contains;
using ::testing::Pair;

TEST_P(MultipleOutputsProcessorTestFixture, TracksProcessPerOutput) {
    set_tee_output();
    auto batch = make_records(1);
    auto initial_batch_offset = push_batch(batch);
    for (auto output : output_topics()) {
        auto returned = read_records(output, 1);
        EXPECT_THAT(returned, SameRecords(batch));
    }
    ASSERT_TRUE(wait_for_committed_offset(initial_batch_offset));
    // Pause writes for the last output
    auto last = output_topics().back();
    cork_sink(last);
    // Push a batch that all sinks get, but the last sink pauses on
    auto corked_batch = make_records(1);
    auto corked_offset = push_batch(corked_batch);
    for (auto output : output_topics()) {
        if (output == last) {
            tests::drain_task_queue().get();
            // We didn't make progress because the write is blocked
            EXPECT_TRUE(sink_empty(output));
            EXPECT_THAT(
              committed_offsets(),
              Contains(Pair(output, initial_batch_offset)));
        } else {
            auto returned = read_records(output, 1);
            EXPECT_THAT(returned, SameRecords(corked_batch));
            EXPECT_TRUE(wait_for_committed_offset(output, corked_offset));
        }
    }
    // Make progress without the last sink, which is stuck.
    auto latest_batch = make_records(1);
    auto latest_offset = push_batch(latest_batch);
    for (auto output : output_topics()) {
        if (output == last) {
            tests::drain_task_queue().get();
            // We didn't make progress because the write is blocked
            EXPECT_TRUE(sink_empty(output));
            EXPECT_THAT(
              committed_offsets(),
              Contains(Pair(output, initial_batch_offset)));
        } else {
            auto returned = read_records(output, 1);
            EXPECT_THAT(returned, SameRecords(latest_batch));
            EXPECT_TRUE(wait_for_committed_offset(output, latest_offset));
        }
    }
    // Attempt to stop, making as much progress as we can, but we won't be able
    // to complete stop as a sink is still corked.
    auto stop_fut = initiate_stop();
    ::tests::drain_task_queue().get();
    // Uncork the sink so that last sink commits the batch it was stuck on.
    uncork_sink(last);
    stop_fut.get();
    bool last_did_commit = false;
    for (auto output : output_topics()) {
        if (output == last) {
            auto returned = read_records(output, 1);
            EXPECT_THAT(returned, SameRecords(corked_batch));
            // We can't ensure that the producer picked up the progress message
            // before it stopped.
            //
            // Pragmatically speaking debug mode will likely not commit and
            // release mode will likely commit, so we get coverage of both
            // cases.
            last_did_commit = wait_for_committed_offset(output, corked_offset);
        } else {
            EXPECT_TRUE(sink_empty(output));
            EXPECT_TRUE(wait_for_committed_offset(output, latest_offset));
        }
    }
    // Start it back up and the last process should catch back up.
    start();
    if (!last_did_commit) {
        // Then we will replay the corked batch.
        auto returned = read_records(last, 1);
        EXPECT_THAT(returned, SameRecords(corked_batch));
        EXPECT_TRUE(wait_for_committed_offset(last, corked_offset));
    }
    // The last record will catchup to the others
    auto returned = read_records(last, 1);
    EXPECT_THAT(returned, SameRecords(latest_batch));
    EXPECT_TRUE(wait_for_all_committed());

    // Other outputs don't emit duplicates
    batch = make_records(1);
    push_batch(batch);
    for (auto output : output_topics()) {
        auto returned = read_records(output, 1);
        EXPECT_THAT(returned, SameRecords(batch));
    }
    EXPECT_TRUE(wait_for_all_committed());
    EXPECT_EQ(error_count(), 0);
}

INSTANTIATE_TEST_SUITE_P(
  MultipleOutputsProcessorTest,
  MultipleOutputsProcessorTestFixture,
  ::testing::Values(fixture_param{testing::my_multiple_output_metadata}));

} // namespace transform
