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

#include "relay/tcp_subscription.h"

#include "base/vlog.h"
#include "relay/logger.h"

#include <seastar/core/coroutine.hh>

#include <arpa/inet.h>

#include <exception>

namespace relay {

ss::future<> tcp_subscription::flush_one(const iobuf& data) {
    uint32_t len = htonl(data.size_bytes());
    // NOLINTNEXTLINE(*-reinterpret-*)
    co_await _out.write(reinterpret_cast<const char*>(&len), sizeof(len));
    for (const auto& frag : data) {
        co_await _out.write(frag.get(), frag.size());
    }
    co_await _out.flush();
}

ss::future<> tcp_subscription::run() {
    try {
        while (true) {
            co_await _cond.wait(_server_as, [this] { return !_queue.empty(); });
            while (!_queue.empty()) {
                auto data = std::move(_queue.front());
                _queue.pop_front();
                co_await flush_one(data);
            }
        }
    } catch (const ss::abort_requested_exception&) {
        // Server shutting down - the normal way run() ends on stop().
    } catch (const std::exception& e) {
        // A write failure (peer closed, network error) just ends this
        // subscription - the relay's other consumers and the producer are
        // unaffected.
        vlog(rlog.debug, "relay tcp consumer disconnected: {}", e.what());
    }
    // Close our end so the peer sees a clean shutdown rather than a hang.
    _conn.shutdown_input();
    _conn.shutdown_output();
}

} // namespace relay
