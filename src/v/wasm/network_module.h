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

#include "utils/unresolved_address.h"
#include "wasm/ffi.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/net/api.hh>

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <vector>

namespace wasm {

/**
 * The WASM host module for real outbound networking, available only to a
 * wasm binary specifically granted the `network` capability in
 * config::wasm_trusted_modules (see wasmtime.cc's engine construction,
 * which is the only place this module gets constructed and linked in).
 *
 * This is deliberately not a raw sockets ABI: a guest can only dial one of
 * `allowed_targets` (by index - it never sees or chooses a real address),
 * and every target on that list was itself put there by a cluster admin,
 * not by whoever deployed this transform. There is no way, from inside
 * this module's ABI, for a trusted guest to reach anywhere its own
 * allowlist doesn't already name.
 */
class network_module {
public:
    explicit network_module(
      std::vector<net::unresolved_address> allowed_targets);
    network_module(const network_module&) = delete;
    network_module& operator=(const network_module&) = delete;
    network_module(network_module&&) = default;
    network_module& operator=(network_module&&) = default;
    ~network_module() = default;

    static constexpr std::string_view name = "redpanda_wasm_network";

    // Start ABI exports
    void check_abi_version_0();

    // Dials allowed_targets[target_index]. On SUCCESS, *out_handle is a
    // non-negative handle to pass to send/recv/close. Fails closed:
    // anything other than a known, in-range index is rejected before any
    // network activity happens.
    ss::future<int32_t> connect(uint32_t target_index, int32_t* out_handle);

    // Writes the entirety of buf to the connection identified by handle.
    ss::future<int32_t> send(int32_t handle, ffi::array<uint8_t> buf);

    // Reads up to buf.size() bytes from the connection into buf. *out_len
    // is the actual number of bytes read, which may be 0 on a graceful
    // remote close - that is not itself an error.
    ss::future<int32_t>
    recv(int32_t handle, ffi::array<uint8_t> buf, uint32_t* out_len);

    // Closes the connection. A no-op, not an error, if handle is unknown -
    // mirrors POSIX close() so guest cleanup code doesn't need to track
    // whether it already closed something.
    ss::future<int32_t> close(int32_t handle);

    // End ABI exports

    // Aborts any in-flight connect and closes every open connection. Called
    // once, from the owning engine's stop() - not part of the guest ABI.
    ss::future<> stop();

private:
    // Owns both the socket and the two stream wrappers layered on it,
    // constructed once at connect time and reused for the connection's
    // whole lifetime - net::connection (src/v/net/connection.h) follows the
    // same shape for the same reason: connected_socket::input()/output()
    // each hand back a fresh stream wrapper on every call, so calling
    // either more than once per connection silently splits buffered state
    // across wrapper instances instead of sharing it.
    struct open_connection {
        explicit open_connection(ss::connected_socket s)
          : socket(std::move(s))
          , in(socket.input())
          , out(socket.output()) {}

        ss::connected_socket socket;
        ss::input_stream<char> in;
        ss::output_stream<char> out;
    };

    std::vector<net::unresolved_address> _allowed_targets;
    absl::flat_hash_map<int32_t, open_connection> _connections;
    int32_t _next_handle{0};
    ss::abort_source _as;
};

} // namespace wasm
