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
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
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
    // registered via shared_memory_module, if any - used only by
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

    // Enqueues buf to be written to the connection identified by handle -
    // does NOT perform the write itself. Copies buf (guest memory, only
    // valid for the duration of this call - same reasoning as
    // transform_module::write_record) into an owned buffer and returns
    // immediately; the actual write happens in drain_pending_pushes(),
    // never inline here, so a slow or unreachable peer can never make
    // this call itself block. Returns BUFFER_FULL (not the usual
    // IO_ERROR) if the pending-push queue is already at its cap - a
    // guest that cares can treat this like any other error and back off
    // or reconnect; one that doesn't just loses this one push, which is
    // the expected cost of a best-effort side-channel under sustained
    // backpressure.
    int32_t send(int32_t handle, ffi::array<uint8_t> buf);

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
    // for one-shot rehydration pulls (fetch a whole snapshot, then close);
    // an ongoing streaming subscription instead uses plain connect/send/
    // recv, kept open across transform() calls.
    ss::future<int32_t>
    bulk_load(uint32_t target_index, ffi::array<uint8_t> request);
    // End ABI exports

    // Not part of the guest ABI - called once per batch by the owning
    // engine (wasmtime.cc), at the same per-batch suspension point
    // transform_module::drain_pending_writes() already uses, right after
    // the guest's invocation for that batch completes.
    //
    // Dispatches (but deliberately does not await) a write for every
    // push enqueued by send() since the last call, one per connection
    // that doesn't already have a push in flight - see push_in_flight
    // below for why "already in flight" means "drop this one" rather
    // than "queue behind it". This function itself never suspends on
    // network I/O, by design: that's the entire point of this being
    // separate from send() at all. A connection whose write eventually
    // fails is torn down once that failure is actually observed, in the
    // background - never synchronously from here, and never while a
    // write against the same connection might still be in flight
    // (output_stream is not safe to operate on concurrently with itself
    // - see the warning at net/batched_output_stream.h:93 against trying
    // to force this via shutdown_output()).
    void drain_pending_pushes();

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
        // Set for the duration of a single dispatched write+flush kicked
        // off from drain_pending_pushes(), cleared once that write's
        // future actually resolves (success or failure) - never while it
        // might still be running. Guards the only invariant that
        // actually matters here: never call anything on `out` while a
        // previous call on the same `out` hasn't returned yet.
        bool push_in_flight = false;
        // Pushes that arrived while push_in_flight was already true,
        // concatenated in arrival order. Drained and sent as one
        // combined write the moment the in-flight write completes,
        // rather than dropped - see dispatch_push()'s completion
        // continuation. Bounded by max_pending_batch_bytes so a
        // persistently slow/wedged peer still degrades by dropping
        // eventually instead of accumulating without limit.
        bytes pending_batch;
    };

    // A push enqueued by send(), not yet handed to the connection's
    // output_stream. handle is re-validated against _connections at
    // drain time, not enqueue time - a handle can go from valid to
    // closed in between (guest-initiated close(), or this same
    // connection failing an earlier push in the same drain pass).
    struct pending_push {
        int32_t handle;
        bytes data;
    };

    // Shared by connect() and bulk_load() - resolves and dials target,
    // honoring _as so an in-flight dial gets aborted on stop(). Throws on
    // a dial failure; callers translate that into the appropriate error
    // code. Callers are responsible for validating target_index against
    // _allowed_targets before calling this.
    ss::future<ss::connected_socket>
    dial(const net::unresolved_address& target);

    // Fire-and-forget dispatch of a single write+flush against handle's
    // connection, marking it push_in_flight for the duration. Called
    // both from drain_pending_pushes() (first write to an idle
    // connection) and from its own completion continuation (draining
    // whatever accumulated in that connection's pending_batch while
    // this write was running) - never called on a connection that's
    // already push_in_flight, same invariant as before.
    void dispatch_push(int32_t handle, bytes data);

    std::vector<net::unresolved_address> _allowed_targets;
    absl::flat_hash_map<int32_t, open_connection> _connections;
    int32_t _next_handle{0};
    ss::abort_source _as;
    std::function<bool(bytes_view)> _shared_memory_sink;

    ss::chunked_fifo<pending_push> _pending_pushes;
    // Every write dispatched from drain_pending_pushes() runs under this
    // gate, never awaited by the caller - stop() closes this gate before
    // it touches _connections directly, so a background push can never
    // still be running (and therefore never races) stop()'s own
    // connection teardown.
    ss::gate _push_gate;
};

} // namespace wasm
