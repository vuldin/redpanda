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

#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "model/transform.h"
#include "relay/probe.h"
#include "relay/relay_service.h"
#include "test_utils/random_bytes.h"
#include "transform/relay_source.h"

#include <seastar/core/abort_source.hh>

#include <gtest/gtest.h>

#include <vector>

// relay_source had NO test coverage until this file (2026-08-29), which was
// noticed only when a change to its pending queue needed verifying. The queue
// is on the relay hot path and carries the delivery-loss semantics the whole
// best-effort contract rests on, so it is worth pinning down directly rather
// than only end-to-end on a cluster.
namespace transform {
namespace {

model::ntp test_ntp() {
    return {
      model::kafka_namespace,
      model::topic{"relay_source_test_topic"},
      model::partition_id{0}};
}

class relay_source_test : public ::testing::Test {
public:
    void SetUp() override {
        // Default the property off for every test; the enabled cases opt in
        // explicitly so a leaked value from one test cannot silently satisfy
        // another's assertion.
        set_stage_metrics(false);
        _svc.emplace(relay::service::config{.max_queue_size = _max_queue});
        _src.emplace(&_svc.value(), test_ntp());
        _src->start().get();
    }

    void TearDown() override {
        if (_src) {
            _src->stop().get();
        }
        set_stage_metrics(false);
    }

    static void set_stage_metrics(bool v) {
        ::config::shard_local_cfg().relay_stage_metrics_enabled.set_value(v);
    }

    // Push exactly what a transform processor pushes: the serialized record
    // payload from transformed_data, NOT arbitrary bytes. on_push runs
    // create_validated on it and silently drops anything that does not parse,
    // so a test using random bytes would assert on an empty queue and pass for
    // the wrong reason.
    void push_records(int n) {
        for (int i = 0; i < n; ++i) {
            auto rec = model::test::make_random_record(
              i, tests::random_iobuf());
            auto td = model::transformed_data::from_record(std::move(rec));
            _svc->push(test_ntp(), td.value());
        }
    }

    int drain_record_count() {
        ss::abort_source as;
        auto reader = _src->read_batch(kafka::offset{0}, &as).get();
        auto batches = model::consume_reader_to_memory(
                         std::move(reader), model::no_timeout)
                         .get();
        int total = 0;
        for (const auto& b : batches) {
            total += b.record_count();
        }
        return total;
    }

    size_t consume_delay_samples() {
        return _svc->get_probe()
          .consume_delay()
          .public_histogram_logform()
          .sample_count;
    }

    size_t emit_to_guest_samples() {
        return _svc->get_probe()
          .emit_to_guest()
          .public_histogram_logform()
          .sample_count;
    }

    int64_t emit_to_guest_mean_us() {
        auto h = _svc->get_probe().emit_to_guest().public_histogram_logform();
        return h.sample_count ? int64_t(h.sample_sum / h.sample_count) : -1;
    }

    int64_t consume_delay_mean_us() {
        auto h = _svc->get_probe().consume_delay().public_histogram_logform();
        return h.sample_count ? int64_t(h.sample_sum / h.sample_count) : -1;
    }

    size_t _max_queue = 1024;
    std::optional<relay::service> _svc;
    std::optional<relay_source> _src;
};

TEST_F(relay_source_test, delivers_every_pushed_record) {
    push_records(3);
    EXPECT_EQ(drain_record_count(), 3);
}

TEST_F(relay_source_test, drain_empties_the_queue) {
    push_records(2);
    EXPECT_EQ(drain_record_count(), 2);
    // A second drain with nothing pushed must yield nothing rather than
    // re-delivering: read_batch moves out of the queue.
    EXPECT_EQ(drain_record_count(), 0);
}

TEST_F(relay_source_test, bounded_queue_drops_when_backlogged) {
    // Rebuild with a tiny bound so the drop path is reachable.
    _src->stop().get();
    _src.reset();
    _svc.reset();
    _svc.emplace(relay::service::config{.max_queue_size = 2});
    _src.emplace(&_svc.value(), test_ntp());
    _src->start().get();

    push_records(5);
    // Bounded, not unbounded: the oldest are dropped, so a drain yields at
    // most the bound rather than all five.
    EXPECT_LE(drain_record_count(), 2);
}

TEST_F(relay_source_test, consume_delay_recorded_only_when_enabled) {
    // Off (the default): the timestamp is never taken and nothing is recorded.
    push_records(4);
    EXPECT_EQ(drain_record_count(), 4);
    EXPECT_EQ(consume_delay_samples(), 0u)
      << "consume_delay was recorded while relay_stage_metrics_enabled=false; "
         "the gate is what keeps two clock reads off the relay hot path";

    // On: one sample per delivered record.
    set_stage_metrics(true);
    push_records(4);
    EXPECT_EQ(drain_record_count(), 4);
    EXPECT_EQ(consume_delay_samples(), 4u);
}

TEST_F(relay_source_test, flipping_the_flag_mid_queue_records_only_the_new) {
    // Records enqueued while the flag was off carry a default-constructed
    // timestamp. Turning the flag on before the drain must NOT turn those into
    // garbage samples measured from the clock epoch.
    push_records(3);
    set_stage_metrics(true);
    push_records(2);
    EXPECT_EQ(drain_record_count(), 5);
    EXPECT_EQ(consume_delay_samples(), 2u)
      << "a record enqueued while the flag was off produced a sample; its "
         "delay would be measured from the epoch, not from its enqueue";
}


// emit_to_guest is the span that replaced three inferred terms, so it needs its
// own test rather than being trusted because the pieces it subsumes are tested.
//
// The invariant that makes it meaningful: emit_to_guest STRICTLY CONTAINS
// consume_delay. consume_delay runs from the record being enqueued on the
// destination to it being dequeued; emit_to_guest starts earlier, at the
// producing transform's emit. So emit >= consume always, and if the wiring ever
// passed the wrong timestamp through (dispatched_at instead of pushed_at, say)
// this is the assertion that would catch it.
TEST_F(relay_source_test, emit_to_guest_contains_consume_delay) {
    set_stage_metrics(true);
    push_records(8);
    ASSERT_EQ(drain_record_count(), 8);

    EXPECT_GT(emit_to_guest_samples(), 0u)
      << "emit_to_guest recorded nothing - pushed_at is probably not reaching "
         "relay_source::on_push through subscription::deliver";
    EXPECT_EQ(emit_to_guest_samples(), consume_delay_samples())
      << "the two spans must be recorded once per record each";
    EXPECT_GE(emit_to_guest_mean_us(), consume_delay_mean_us())
      << "emit_to_guest must CONTAIN consume_delay (emit happens before the "
         "enqueue), so it can never be the smaller of the two";
}

// Same gate as every other stage histogram: recording costs clock reads on the
// relay hot path, so it must stay off unless asked for.
TEST_F(relay_source_test, emit_to_guest_not_recorded_when_metrics_off) {
    set_stage_metrics(false);
    push_records(8);
    ASSERT_EQ(drain_record_count(), 8);
    EXPECT_EQ(emit_to_guest_samples(), 0u)
      << "emit_to_guest recorded while relay_stage_metrics_enabled=false";
}

} // namespace

} // namespace transform
