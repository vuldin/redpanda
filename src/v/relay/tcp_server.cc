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

#include "relay/tcp_server.h"

#include "base/vlog.h"
#include "model/namespace.h"
#include "relay/logger.h"
#include "relay/relay_service.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/inet_address.hh>

#include <arpa/inet.h>

#include <exception>

namespace relay {

ss::future<> tcp_server::start() {
    ss::listen_options lo;
    lo.reuse_address = true;
    _listener = ss::engine().listen(
      {ss::net::inet_address("0.0.0.0"), _port}, lo);
    vlog(rlog.info, "relay tcp server listening on port {}", _port);
    ssx::background = accept_loop();
    co_return;
}

ss::future<> tcp_server::stop() {
    if (!_as.abort_requested()) {
        // Firing _as makes every subscription's run() return; aborting the
        // listener's accept makes the accept loop return. Both then release
        // the gate below.
        _as.request_abort();
    }
    if (_listener) {
        _listener->abort_accept();
    }
    co_await _gate.close();
}

ss::future<> tcp_server::accept_loop() {
    auto holder = _gate.hold();
    while (!_as.abort_requested()) {
        ss::accept_result ar;
        try {
            ar = co_await _listener->accept();
        } catch (const std::exception& e) {
            if (_as.abort_requested()) {
                break;
            }
            vlog(rlog.warn, "relay tcp accept failed: {}", e.what());
            continue;
        }
        ssx::background = handle_connection(std::move(ar.connection));
    }
}

ss::future<> tcp_server::handle_connection(ss::connected_socket conn) {
    auto holder = _gate.hold();
    std::unique_ptr<tcp_subscription> sub;
    tcp_subscription* sub_ptr = nullptr;
    uint32_t id = 0;
    bool registered = false;
    try {
        // One input wrapper for the connection's lifetime (see the warning
        // at network_module.h about input()/output() handing back a fresh
        // wrapper per call) - used here for the subscribe request, then the
        // socket moves into the subscription which owns it from then on.
        auto in = conn.input();
        // Subscribe request: [2-byte topic length][topic][4-byte partition].
        auto len_buf = co_await in.read_exactly(2);
        uint16_t topic_len = ntohs(
          // NOLINTNEXTLINE(*-reinterpret-*)
          *reinterpret_cast<const uint16_t*>(len_buf.get()));
        auto topic_buf = co_await in.read_exactly(topic_len);
        auto partition_buf = co_await in.read_exactly(4);
        uint32_t partition = ntohl(
          // NOLINTNEXTLINE(*-reinterpret-*)
          *reinterpret_cast<const uint32_t*>(partition_buf.get()));
        ss::sstring topic(topic_buf.get(), topic_len);

        model::ntp ntp{
          model::kafka_namespace,
          model::topic{std::move(topic)},
          model::partition_id(partition)};
        sub = std::make_unique<tcp_subscription>(
          std::move(conn), _max_queue_size, _as);
        sub_ptr = sub.get();
        id = _relay->add_subscription(std::move(ntp), sub_ptr);
        registered = true;
        vlog(rlog.debug, "relay tcp consumer subscribed: id {}", id);
    } catch (const std::exception& e) {
        vlog(rlog.debug, "relay tcp consumer subscribe failed: {}", e.what());
        co_return;
    }

    // Streams records until the client disconnects or the server stops.
    co_await sub_ptr->run();

    if (registered) {
        _relay->remove_subscription(id);
    }
    // sub (unique_ptr) destroys the subscription here, after the relay can
    // no longer call deliver() on it.
}

} // namespace relay
