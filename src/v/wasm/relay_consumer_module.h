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

#pragma once

#include "absl/container/flat_hash_map.h"
#include "base/seastarx.h"
#include "model/fundamental.h"
#include "relay/fwd.h"
#include "relay/subscription.h"
#include "wasm/ffi.h"
#include "wasm/fwd.h"

#include <seastar/core/future.hh>

#include <cstdint>
#include <memory>

namespace wasm {

/**
 * The WASM host module letting a trusted guest subscribe to relay-pushed
 * data (see src/v/relay/) for a (topic, partition), delivered into the
 * guest's registered shared-memory region - the in-process, zero-network-hop
 * counterpart to the TCP delivery external relay consumers use.
 *
 * Only linked into a binary specifically granted the `relay_consumer`
 * capability in config::wasm_trusted_modules, and only useful alongside
 * `shared_memory` (the relay delivers via engine::write_shared_memory, so a
 * guest that never registers a region simply drops whatever it would have
 * received).
 *
 * The module holds no per-record state: subscribe()/unsubscribe() only
 * (de)register a relay::subscription with the shard-local relay. Delivery
 * itself is driven by the relay calling back into the engine, entirely
 * outside this module's ABI.
 */
class relay_consumer_module {
public:
    relay_consumer_module(relay::service* relay, engine* eng);
    relay_consumer_module(const relay_consumer_module&) = delete;
    relay_consumer_module& operator=(const relay_consumer_module&) = delete;
    relay_consumer_module(relay_consumer_module&&) = delete;
    relay_consumer_module& operator=(relay_consumer_module&&) = delete;
    ~relay_consumer_module() = default;

    static constexpr std::string_view name = "redpanda_wasm_relay_consumer";

    // Start ABI exports
    void check_abi_version_0();

    // Subscribe to (topic, partition) on this shard's relay. Returns a
    // positive consumer id on success, or a negative error. Data pushed to
    // the relay for that stream is written into the guest's registered
    // shared-memory region (overwriting whatever was there - the guest is
    // expected to consume it promptly).
    int32_t subscribe(ffi::array<uint8_t> topic, uint32_t partition);

    // Remove a subscription previously returned by subscribe(). A no-op, not
    // an error, for an unknown id.
    int32_t unsubscribe(uint32_t consumer_id);
    // End ABI exports

    // Unsubscribes everything this guest subscribed. Called once from the
    // owning engine's stop(), before the engine (and therefore the
    // shared-memory region the relay writes into) is torn down - this is
    // what keeps the relay from ever delivering into a dead engine.
    ss::future<> stop();

private:
    relay::service* _relay;
    engine* _engine;
    // The relay is non-owning (see relay::service::add_subscription), so the
    // subscriptions this guest registered live here, keyed by the id the
    // relay handed back. They're removed from the relay before being
    // destroyed, in stop() and unsubscribe().
    absl::flat_hash_map<uint32_t, std::unique_ptr<relay::subscription>>
      _subscriptions;
};

} // namespace wasm
