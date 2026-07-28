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

#include <cstdint>
#include <optional>
#include <string_view>

namespace wasm {

// One guest-registered region: a byte range, in the guest's own linear
// memory, that the host may write into at any time - not just during a
// host function call. ptr/len are guest-relative offsets, deliberately not
// an already-translated host pointer: the underlying wasm memory can grow
// (and therefore move) at any point after registration, so every actual
// write re-translates these fresh via wasmtime_memory_data(), the same way
// every other host function's ffi::array parameters already do.
struct shared_memory_region {
    uint32_t ptr;
    uint32_t len;
};

/**
 * The WASM host module for host-initiated writes into a guest-designated
 * region of its own linear memory, available only to a wasm binary
 * specifically granted the `shared_memory` capability in
 * config::wasm_trusted_modules (see wasmtime.cc's engine construction).
 *
 * This is the fast-path counterpart to network_module's call-out: instead
 * of a guest paying a host-call round trip on every lookup (e.g. checking
 * a reference-data cache), the host can push current data directly into a
 * region the guest reads with a plain memory load. This module is only the
 * registration ABI - the actual push is wasmtime_engine::write_shared_memory
 * (see wasm/engine.h), a host-internal operation with no guest-facing
 * counterpart, callable at any time, not gated on any guest call happening.
 */
class shared_memory_module {
public:
    shared_memory_module() = default;
    shared_memory_module(const shared_memory_module&) = delete;
    shared_memory_module& operator=(const shared_memory_module&) = delete;
    shared_memory_module(shared_memory_module&&) = default;
    shared_memory_module& operator=(shared_memory_module&&) = default;
    ~shared_memory_module() = default;

    static constexpr std::string_view name = "redpanda_wasm_shared_memory";

    // Start ABI exports
    void check_abi_version_0();

    // Registers [ptr, ptr + len) as the region the host may write into.
    // Deliberately does not touch guest memory itself - no bounds check
    // happens here, because there is nothing yet to bounds-check against
    // safely without also being ready to immediately act on a stale
    // result (see the struct comment on shared_memory_region). The one
    // real bounds check happens in write_shared_memory, immediately before
    // the one place an out-of-range offset would actually matter.
    // Replaces any previously registered region - a guest that wants to
    // change its region size just registers a new one.
    int32_t register_region(uint32_t ptr, uint32_t len);
    // End ABI exports

    const std::optional<shared_memory_region>& region() const noexcept {
        return _region;
    }

private:
    std::optional<shared_memory_region> _region;
};

} // namespace wasm
