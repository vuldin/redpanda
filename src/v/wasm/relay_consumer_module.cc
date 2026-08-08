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

#include "relay_consumer_module.h"

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "logger.h"
#include "model/namespace.h"
#include "relay/relay_service.h"
#include "relay/subscription.h"
#include "wasm/engine.h"

#include <seastar/core/coroutine.hh>

namespace wasm {

namespace {

constexpr int32_t SUCCESS = 0;

// Delivers relayed bytes into the guest's registered shared-memory region.
// Lives entirely on the shard that owns both the relay stream and the
// engine, so deliver() is a plain synchronous call from the relay's push
// path - linearizing the iobuf here is the one copy this delivery makes,
// and only happens when this guest is actually subscribed.
class wasm_relay_subscription final : public relay::subscription {
public:
    explicit wasm_relay_subscription(engine* eng)
      : _engine(eng) {}

    bool deliver(const iobuf& data) final {
        auto bytes = iobuf_to_bytes(data);
        // Returns false (dropped) if the guest hasn't registered a region
        // yet, or this engine wasn't granted shared_memory - the relay
        // counts it and moves on, the producer is never told to slow down.
        return _engine->write_shared_memory(bytes);
    }

private:
    engine* _engine;
};

} // namespace

relay_consumer_module::relay_consumer_module(relay::service* relay, engine* eng)
  : _relay(relay)
  , _engine(eng) {}

void relay_consumer_module::check_abi_version_0() {}

int32_t relay_consumer_module::subscribe(
  ffi::array<uint8_t> topic, uint32_t partition) {
    model::topic_namespace nt{
      model::kafka_namespace,
      model::topic{std::string_view{
        reinterpret_cast<const char*>(topic.data()), topic.size()}}};
    auto sub = std::make_unique<wasm_relay_subscription>(_engine);
    auto id = _relay->add_subscription(
      model::ntp{nt.ns, nt.tp, model::partition_id(partition)}, sub.get());
    _subscriptions.emplace(id, std::move(sub));
    vlog(
      wasm_log.debug,
      "relay consumer subscribed to {}/{}: id {}",
      nt.tp,
      partition,
      id);
    // Guest-facing ids are 1-based so 0 stays a usable "invalid" sentinel;
    // the relay's own ids are 0-based. Subscription ids increase from 0, so
    // the int32_t cast is safe for any realistic count.
    return static_cast<int32_t>(id) + 1;
}

int32_t relay_consumer_module::unsubscribe(uint32_t consumer_id) {
    if (consumer_id == 0) {
        return SUCCESS;
    }
    auto relay_id = consumer_id - 1;
    auto it = _subscriptions.find(relay_id);
    if (it == _subscriptions.end()) {
        return SUCCESS;
    }
    // Remove from the relay first - only once the relay can no longer call
    // deliver() on it is it safe to destroy the subscription.
    _relay->remove_subscription(relay_id);
    _subscriptions.erase(it);
    return SUCCESS;
}

ss::future<> relay_consumer_module::stop() {
    for (auto& [id, _] : _subscriptions) {
        _relay->remove_subscription(id);
    }
    _subscriptions.clear();
    co_return;
}

} // namespace wasm
