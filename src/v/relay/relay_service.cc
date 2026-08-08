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

#include "relay/relay_service.h"

#include "base/vlog.h"
#include "relay/logger.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

namespace relay {

service::service() = default;
service::service(config cfg)
  : _cfg(cfg) {}
service::~service() = default;

ss::future<> service::start() {
    _probe.setup_metrics();
    if (_cfg.tcp_enabled) {
        // Each shard listens on base port + shard id, so a consumer connects
        // to the shard that owns its partition's data (shard-local, no
        // cross-shard hops in the push path).
        uint16_t port = _cfg.tcp_port + ss::this_shard_id();
        _tcp.emplace(this, port, _cfg.max_queue_size);
        co_await _tcp->start();
    }
    co_return;
}

ss::future<> service::stop() {
    if (_tcp) {
        co_await _tcp->stop();
        _tcp.reset();
    }
    _by_ntp.clear();
    _ntp_by_id.clear();
    co_return;
}

void service::push(const model::ntp& ntp, const iobuf& data) {
    auto it = _by_ntp.find(ntp);
    if (it == _by_ntp.end()) {
        return;
    }
    _probe.increment_pushes();
    for (auto& [_, sub] : it->second) {
        if (sub->deliver(data)) {
            _probe.increment_delivered();
        } else {
            _probe.increment_dropped();
        }
    }
}

uint32_t service::add_subscription(model::ntp ntp, subscription* sub) {
    auto id = _next_id++;
    _by_ntp[ntp].emplace(id, sub);
    _ntp_by_id.emplace(id, std::move(ntp));
    _probe.subscription_added();
    return id;
}

void service::remove_subscription(uint32_t id) {
    auto ntp_it = _ntp_by_id.find(id);
    if (ntp_it == _ntp_by_id.end()) {
        return;
    }
    auto it = _by_ntp.find(ntp_it->second);
    if (it != _by_ntp.end()) {
        it->second.erase(id);
        if (it->second.empty()) {
            _by_ntp.erase(it);
        }
    }
    _ntp_by_id.erase(ntp_it);
    _probe.subscription_removed();
}

size_t service::subscription_count(const model::ntp& ntp) const {
    auto it = _by_ntp.find(ntp);
    return it == _by_ntp.end() ? 0 : it->second.size();
}

} // namespace relay
