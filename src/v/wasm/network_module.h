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

#include "bytes/bytes.h"
#include "utils/unresolved_address.h"
#include "wasm/ffi.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/net/api.hh>

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <functional>
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
    // shared_memory_sink delivers bytes into whatever region the guest has
    // registered via shared_memory_module (PR-13c), if any - used only by
    // bulk_load, and only ever actually linked into a guest's imports when
    // both the `network` and `shared_memory` capabilities are granted (see
    // wasmtime.cc). Always real (never null) when a network_module exists
    // at all: wasmtime_engine::write_shared_memory already returns false,
    // harmlessly, if shared_memory wasn't granted or no region is
    // registered yet, so there is nothing extra to gate here.
    network_module(
      std::vector<net::unresolved_address> allowed_targets,
      std::function<bool(bytes_view)> shared_memory_sink);
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

    // One-shot bulk pull: dials allowed_targets[target_index], sends
    // request, then reads the full response - looping host-side, not via
    // N guest ABI calls - until the peer closes the connection or a
    // size/time bound is hit, and delivers the result through the
    // shared_memory_sink rather than back through this call directly, so
    // an arbitrarily large response never needs to fit in a single ffi
    // buffer. Returns the number of bytes delivered on success. This is
    // for EP3-style one-shot rehydration pulls (e.g. SnapshotAPI.
    // GetSnapshot) - the ongoing streaming subscriptions (ListenInstruments
    // / StreamState / StreamTrades) still use plain connect/send/recv, kept
    // open across transform() calls.
    ss::future<int32_t>
    bulk_load(uint32_t target_index, ffi::array<uint8_t> request);
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

    // Shared by connect() and bulk_load() - resolves and dials target,
    // honoring _as so an in-flight dial gets aborted on stop(). Throws on
    // a dial failure; callers translate that into the appropriate error
    // code. Callers are responsible for validating target_index against
    // _allowed_targets before calling this.
    ss::future<ss::connected_socket>
    dial(const net::unresolved_address& target);

    std::vector<net::unresolved_address> _allowed_targets;
    absl::flat_hash_map<int32_t, open_connection> _connections;
    int32_t _next_handle{0};
    ss::abort_source _as;
    std::function<bool(bytes_view)> _shared_memory_sink;
};

} // namespace wasm
