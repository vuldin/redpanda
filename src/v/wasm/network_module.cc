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

#include "network_module.h"

#include "base/vlog.h"
#include "logger.h"
#include "net/dial.h"
#include "net/dns.h"

#include <seastar/core/coroutine.hh>

#include <chrono>
#include <exception>

namespace wasm {

namespace {
using namespace std::chrono_literals;

// A TCP handshake is one RTT to an operator-declared internal or partner
// endpoint, not an arbitrary Internet host; the same fixed timeout
// http::client::dial uses for the same reason - fail fast, let the guest's
// own retry logic (or lack of one) decide what happens next, rather than
// have a single slow endpoint hold a wasm engine's one mutex-serialized
// instance (see G5 in the wasm roadmap doc) hostage for longer than that.
constexpr auto connect_timeout = 1s;

// Bounds the blast radius of a single instance opening connections faster
// than it closes them. This is a per-instance limit only - it does not by
// itself bound how many instances of one transform, or how many different
// trusted transforms, can share one operation's real-world target; that is
// exactly the amplification question the PR-13 design doc flags as needing
// a real security review before this ships, not something this constant
// resolves on its own.
constexpr size_t max_connections_per_instance = 8;

constexpr int32_t SUCCESS = 0;
constexpr int32_t INVALID_TARGET_INDEX = -1;
constexpr int32_t CONNECTION_LIMIT_EXCEEDED = -2;
constexpr int32_t CONNECT_FAILED = -3;
constexpr int32_t INVALID_HANDLE = -4;
constexpr int32_t IO_ERROR = -5;
} // namespace

network_module::network_module(
  std::vector<net::unresolved_address> allowed_targets)
  : _allowed_targets(std::move(allowed_targets)) {}

void network_module::check_abi_version_0() {}

ss::future<int32_t>
network_module::connect(uint32_t target_index, int32_t* out_handle) {
    if (target_index >= _allowed_targets.size()) {
        co_return INVALID_TARGET_INDEX;
    }
    if (_connections.size() >= max_connections_per_instance) {
        co_return CONNECTION_LIMIT_EXCEEDED;
    }
    const auto& target = _allowed_targets[target_index];
    try {
        auto addresses = co_await net::resolve_dns_all(target);
        auto socket = co_await net::dial_serially(
          std::move(addresses),
          net::clock_type::now() + connect_timeout,
          net::fixed_timeout_dial_policy{.attempt_timeout = connect_timeout},
          &wasm_log,
          [this] { _as.check(); });
        int32_t handle = _next_handle++;
        _connections.emplace(handle, open_connection(std::move(socket)));
        *out_handle = handle;
        co_return SUCCESS;
    } catch (...) {
        vlog(
          wasm_log.warn,
          "wasm network connect to {} failed: {}",
          target,
          std::current_exception());
        co_return CONNECT_FAILED;
    }
}

ss::future<int32_t>
network_module::send(int32_t handle, ffi::array<uint8_t> buf) {
    auto it = _connections.find(handle);
    if (it == _connections.end()) {
        co_return INVALID_HANDLE;
    }
    try {
        // NOLINTNEXTLINE(*-reinterpret-*)
        co_await it->second.out.write(
          reinterpret_cast<const char*>(buf.data()), buf.size());
        co_await it->second.out.flush();
        co_return SUCCESS;
    } catch (...) {
        vlog(
          wasm_log.warn,
          "wasm network send on handle {} failed: {}",
          handle,
          std::current_exception());
        co_return IO_ERROR;
    }
}

ss::future<int32_t> network_module::recv(
  int32_t handle, ffi::array<uint8_t> buf, uint32_t* out_len) {
    auto it = _connections.find(handle);
    if (it == _connections.end()) {
        co_return INVALID_HANDLE;
    }
    try {
        auto data = co_await it->second.in.read_up_to(buf.size());
        std::copy(data.begin(), data.end(), buf.begin());
        *out_len = data.size();
        co_return SUCCESS;
    } catch (...) {
        vlog(
          wasm_log.warn,
          "wasm network recv on handle {} failed: {}",
          handle,
          std::current_exception());
        co_return IO_ERROR;
    }
}

ss::future<int32_t> network_module::close(int32_t handle) {
    auto it = _connections.find(handle);
    if (it == _connections.end()) {
        co_return SUCCESS;
    }
    auto conn = std::move(it->second);
    _connections.erase(it);
    try {
        co_await conn.out.close();
        co_await conn.in.close();
    } catch (...) {
        // The guest can't act on a close failure any differently - the
        // connection is gone from its point of view either way.
    }
    co_return SUCCESS;
}

ss::future<> network_module::stop() {
    _as.request_abort();
    auto connections = std::exchange(_connections, {});
    for (auto& [handle, conn] : connections) {
        try {
            co_await conn.out.close();
            co_await conn.in.close();
        } catch (...) {
            // Best-effort - we're tearing this instance down regardless.
        }
    }
}

} // namespace wasm
