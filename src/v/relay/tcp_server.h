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

#include "base/seastarx.h"
#include "relay/fwd.h"
#include "relay/tcp_subscription.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/net/api.hh>

#include <cstddef>
#include <cstdint>
#include <optional>

namespace relay {

/**
 * The per-shard TCP listener external relay consumers connect to. One runs
 * on each shard, on (base port + shard id), so a consumer picks the shard
 * that owns its partition's data - the same shard-locality the relay's
 * whole push path relies on, with no cross-shard hops.
 *
 * Wire protocol (client -> server, once on connect):
 *   [2-byte big-endian topic length][topic bytes][4-byte big-endian partition]
 * after which the server streams each relayed record as
 *   [4-byte big-endian length][payload]
 * matching the wasm-orderbook push-relay framing.
 */
class tcp_server {
public:
    tcp_server(service* relay, uint16_t port, size_t max_queue_size)
      : _relay(relay)
      , _port(port)
      , _max_queue_size(max_queue_size) {}

    tcp_server(const tcp_server&) = delete;
    tcp_server& operator=(const tcp_server&) = delete;
    tcp_server(tcp_server&&) = delete;
    tcp_server& operator=(tcp_server&&) = delete;
    ~tcp_server() = default;

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> accept_loop();
    ss::future<> handle_connection(ss::connected_socket);

    service* _relay;
    uint16_t _port;
    size_t _max_queue_size;
    // Shared with every tcp_subscription this server spawns: firing it makes
    // each subscription's run() return, so stop() just aborts and waits on
    // the gate rather than tracking connections individually.
    ss::abort_source _as;
    ss::gate _gate;
    // Kept so stop() can abort the blocking accept() - without that, the
    // accept loop would never notice _as and stop() would hang.
    std::optional<ss::server_socket> _listener;
};

} // namespace relay
