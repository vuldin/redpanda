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
#include "model/namespace.h"
#include "relay/logger.h"
#include "relay/relay_service.h"
#include "relay/subscription.h"

#include <seastar/core/coroutine.hh>

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

private:
    relay_source* _src;
};

relay_source::relay_source(relay::service* relay, model::ntp relay_ntp)
  : _relay(relay)
  , _relay_ntp(std::move(relay_ntp))
  , _sub(std::make_unique<queue_subscription>(this)) {}

relay_source::~relay_source() = default;

ss::future<> relay_source::start() {
    _sub_id = _relay->add_subscription(_relay_ntp, _sub.get());
    vlog(
      relay::rlog.debug,
      "relay source subscribed to {}/{}: id {}",
      _relay_ntp.tp.topic,
      _relay_ntp.tp.partition,
      _sub_id);
    co_return;
}

ss::future<> relay_source::stop() {
    _relay->remove_subscription(_sub_id);
    _data_available.broken();
    co_await _gate.close();
}

bool relay_source::on_push(const iobuf& payload) {
    auto td = model::transformed_data::create_validated(payload.copy());
    if (!td) {
        // A payload that doesn't parse back into a record - drop it rather
        // than poison the consumer's record stream.
        return false;
    }
    _pending.push_back(std::move(*td));
    ++_delivered_count;
    _data_available.signal();
    return true;
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
    auto holder = _gate.hold();
    if (_pending.empty()) {
        co_return model::make_memory_record_batch_reader(
          model::record_batch_reader::data_t{});
    }
    ss::chunked_fifo<model::transformed_data> records;
    while (!_pending.empty()) {
        records.push_back(std::move(_pending.front()));
        _pending.pop_front();
    }
    auto batch = model::transformed_data::make_batch(
      model::timestamp::now(), std::move(records));
    // Stamp a synthetic, monotonically increasing base offset so the consumer
    // loop's progress tracking and lag reporting behave sensibly. These are
    // not Kafka log offsets - the relay has no log - they only order this
    // consumer's own stream.
    batch.header().base_offset = model::offset(_next_offset());
    _next_offset = kafka::offset(
      _next_offset() + batch.header().record_count);
    co_return model::make_memory_record_batch_reader(std::move(batch));
}

ss::future<> relay_source::wait_for_offset(
  kafka::offset,
  model::timeout_clock::time_point deadline,
  ss::abort_source* as) {
    // Block until a push lands in the pending queue (or the deadline/abort
    // fires). This is the event-driven notification that keeps a relay
    // consumer off any fixed poll interval.
    co_await _data_available
      .wait(deadline, *as, [this] { return !_pending.empty(); })
      .handle_exception([](const std::exception_ptr&) {
          // Deadline or abort - the consumer loop re-checks read_batch either
          // way, so swallowing here is correct.
      });
    co_return;
}

} // namespace transform
