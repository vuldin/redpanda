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

#include "shared_memory_module.h"

namespace wasm {

namespace {
constexpr int32_t SUCCESS = 0;
} // namespace

void shared_memory_module::check_abi_version_0() {}

int32_t shared_memory_module::register_region(uint32_t ptr, uint32_t len) {
    _region = shared_memory_region{.ptr = ptr, .len = len};
    return SUCCESS;
}

} // namespace wasm
