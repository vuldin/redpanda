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
#include "bytes/iobuf.h"
#include "relay/subscription.h"
#include "ssx/condition_variable.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/net/api.hh>

#include <cstddef>

namespace relay {

/**
 * A relay subscription that fans data out to one external TCP consumer.
 *
 * deliver() only enqueues - it never writes to the socket inline, so a slow
 * or wedged consumer can never block the producer's hot path. The queue is
 * bounded; once full, the oldest record is dropped to make room, matching
 * the relay's best-effort fast-path contract (the durable Kafka write is
 * unaffected, so a consumer that needs every record can fall back to fetch).
 *
 * The actual socket write happens in run(), a background fiber that frames
 * each record as [4-byte big-endian length][payload] - the same framing the
 * wasm-orderbook push-relay uses, so existing external clients parse it
 * unchanged.
 */
class tcp_subscription final : public subscription {
public:
    // server_as fires when the owning tcp_server shuts down - run() watches
    // it and returns, so the server never has to track or stop individual
    // subscriptions. The socket itself is closed by the connected_socket
    // destructor when this subscription is destroyed.
    tcp_subscription(
      ss::connected_socket conn, size_t max_queue_size, ss::abort_source& server_as)
      : _conn(std::move(conn))
      , _out(_conn.output())
      , _max_queue_size(max_queue_size)
      , _server_as(server_as) {}

    tcp_subscription(const tcp_subscription&) = delete;
    tcp_subscription& operator=(const tcp_subscription&) = delete;
    tcp_subscription(tcp_subscription&&) = delete;
    tcp_subscription& operator=(tcp_subscription&&) = delete;
    ~tcp_subscription() final = default;

    bool deliver(const iobuf& data) final {
        if (_queue.size() >= _max_queue_size) {
            _queue.pop_front();
            ++_dropped;
        }
        _queue.push_back(data.copy());
        _cond.signal();
        return true;
    }

    // Drains the queue to the socket until the client disconnects (a write
    // fails) or the server shuts down (server_as fires).
    ss::future<> run();

    uint64_t dropped() const { return _dropped; }

private:
    ss::future<> flush_one(const iobuf& data);

    ss::connected_socket _conn;
    ss::output_stream<char> _out;
    ss::chunked_fifo<iobuf> _queue;
    size_t _max_queue_size;
    uint64_t _dropped = 0;
    ssx::condition_variable _cond;
    ss::abort_source& _server_as;
};

} // namespace relay
