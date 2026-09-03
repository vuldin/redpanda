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

#include "transform/relay_source.h"

#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "config/configuration.h"
#include "logger.h"
#include "model/namespace.h"
#include "relay/probe.h"
#include "relay/relay_service.h"
#include "relay/subscription.h"

#include <seastar/core/coroutine.hh>

#include <chrono>

namespace transform {

// The relay::subscription a relay_source registers with the relay. deliver()
// just hands the bytes to the owning source's on_push - the relay is
// non-owning (relay::service::add_subscription), so this object lives on the
// source and is unregistered before the source is destroyed.
class relay_source::queue_subscription final : public relay::subscription {
public:
    explicit queue_subscription(relay_source* src)
      : _src(src) {}

    bool deliver(const iobuf& data) final { return _src->on_push(data); }
    bool
    deliver(const iobuf& data, ss::steady_clock_type::time_point pushed_at) final {
        return _src->on_push(data, pushed_at);
    }

private:
    relay_source* _src;
};

relay_source::relay_source(relay::service* relay, model::ntp relay_ntp)
  : _relay(relay)
  , _relay_ntp(std::move(relay_ntp))
  , _sub(std::make_unique<queue_subscription>(this))
  , _max_pending(relay->max_queue_size()) {}

relay_source::~relay_source() = default;

ss::future<> relay_source::start() {
    // Fresh every start(), including a restart on this same instance
    // after a prior stop() - see the header's comment on why these are
    // optional: a plain gate/condition_variable is one-shot, and this
    // processor gets restarted (stop() then start() again on the SAME
    // relay_source), not recreated.
    _data_available.emplace();
    _gate.emplace();
    _sub_id = _relay->add_subscription(_relay_ntp, _sub.get());
    vlog(
      tlog.debug,
      "relay source subscribed to {}/{}: id {}",
      _relay_ntp.tp.topic,
      _relay_ntp.tp.partition,
      _sub_id);
    co_return;
}

ss::future<> relay_source::stop() {
    _relay->remove_subscription(_sub_id);
    if (_data_available) {
        _data_available->broken();
    }
    if (_gate) {
        co_await _gate->close();
    }
}

bool relay_source::on_push(
  const iobuf& payload, ss::steady_clock_type::time_point pushed_at) {
    auto td = model::transformed_data::create_validated(payload.copy());
    if (!td) {
        // A payload that doesn't parse back into a record - drop it rather
        // than poison the consumer's record stream.
        return false;
    }
    bool backlogged = _pending.size() >= _max_pending;
    if (backlogged) {
        // This consumer's guest can't keep up with the producer's push
        // rate - drop the oldest pending record instead of growing without
        // limit, the same bound tcp_subscription applies to its own queue.
        _pending.pop_front();
    }
    // Only read the clock when the histogram is actually being recorded;
    // a default-constructed time_point is read_batch's "skip this" signal.
    auto enqueued_at = config::shard_local_cfg().relay_stage_metrics_enabled()
                         ? ss::steady_clock_type::now()
                         : ss::steady_clock_type::time_point{};
    _pending.push_back({std::move(*td), enqueued_at, pushed_at});
    ++_delivered_count;
    // on_push only ever runs between start() (registers the
    // subscription) and stop() (removes it), so _data_available is
    // always engaged here.
    _data_available->signal();
    return !backlogged;
}

kafka::offset relay_source::latest_offset() {
    // Offsets are 0-indexed per delivery, so the latest delivered offset is
    // one past the count. Matches kafka::offset conventions where the "latest
    // offset" of an empty log is the offset the next write will get.
    return kafka::offset(_delivered_count);
}

ss::future<std::optional<kafka::offset>>
relay_source::offset_at_timestamp(model::timestamp, ss::abort_source*) {
    co_return std::nullopt;
}

kafka::offset relay_source::start_offset() const { return kafka::offset(0); }

ss::future<model::record_batch_reader>
relay_source::read_batch(kafka::offset, ss::abort_source* as) {
    // Only ever called from the transform's own consumer loop, which
    // only runs between start() and stop() - _gate is always engaged.
    auto holder = _gate->hold();
    if (_pending.empty()) {
        co_return model::make_memory_record_batch_reader(
          model::record_batch_reader::data_t{});
    }
    ss::chunked_fifo<model::transformed_data> records;
    // One clock read for the whole drain, not one per record: every record in
    // this batch is being dequeued at the same instant by definition, so
    // per-record reads would measure the drain loop rather than the delay.
    const bool measure
      = config::shard_local_cfg().relay_stage_metrics_enabled();
    auto dequeued_at = measure ? ss::steady_clock_type::now()
                               : ss::steady_clock_type::time_point{};
    while (!_pending.empty()) {
        auto& front = _pending.front();
        if (measure && front.pushed_at != ss::steady_clock_type::time_point{}) {
            // The whole in-broker fan-out path in ONE span: the producing
            // transform's emit through to this dequeue. Quote this rather than
            // a sum of the stage histograms, which overlap and leave gaps.
            _relay->get_probe().record_emit_to_guest(
              std::chrono::duration_cast<std::chrono::microseconds>(
                dequeued_at - front.pushed_at));
        }
        if (measure && front.enqueued_at != ss::steady_clock_type::time_point{}) {
            _relay->get_probe().record_consume_delay(
              std::chrono::duration_cast<std::chrono::microseconds>(
                dequeued_at - front.enqueued_at));
        }
        records.push_back(std::move(front.data));
        _pending.pop_front();
    }
    auto batch = model::transformed_data::make_batch(
      model::timestamp::now(), std::move(records));
    // Stamp a synthetic, monotonically increasing base offset so the consumer
    // loop's progress tracking and lag reporting behave sensibly. These are
    // not Kafka log offsets - the relay has no log - they only order this
    // consumer's own stream.
    batch.header().base_offset = model::offset(_next_offset());
    _next_offset = kafka::offset(_next_offset() + batch.header().record_count);
    co_return model::make_memory_record_batch_reader(std::move(batch));
}

ss::future<> relay_source::wait_for_offset(
  kafka::offset,
  model::timeout_clock::time_point deadline,
  ss::abort_source* as) {
    // Block until a push lands in the pending queue (or the deadline/abort
    // fires). This is the event-driven notification that keeps a relay
    // consumer off any fixed poll interval. Only ever called from the
    // transform's own consumer loop (between start() and stop()), so
    // _data_available is always engaged here.
    co_await _data_available
      ->wait(deadline, *as, [this] { return !_pending.empty(); })
      .handle_exception([](const std::exception_ptr&) {
          // Deadline or abort - the consumer loop re-checks read_batch either
          // way, so swallowing here is correct.
      });
    co_return;
}

} // namespace transform
