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

#include "bytes/bytes.h"
#include "config/configuration.h"
#include "wasm/errc.h"
#include "wasm/tests/wasm_fixture.h"

TEST_F(WasmTestFixture, SharedMemoryUntrustedBinaryFailsToInstantiate) {
    // No config::wasm_trusted_modules entry for this binary, so
    // register_shared_memory_module is never called in make_factory - the
    // guest's own unconditional import of redpanda_wasm_shared_memory's
    // register_region (see testdata/shared-memory/transform.go's main())
    // then can't be satisfied at link time. This is the actual enforcement
    // point this feature relies on: an untrusted binary that tries to use a
    // capability it wasn't granted doesn't get a benign "no-op" response,
    // it fails to even start.
    config::shard_local_cfg().wasm_trusted_modules.set_value(
      std::vector<config::wasm_trusted_module>{});
    EXPECT_THROW(load_wasm("shared-memory.wasm"), wasm::wasm_exception);
}

TEST_F(WasmTestFixture, SharedMemoryTrustedBinaryReadsHostWrites) {
    // load_wasm computes meta().binary_sha256 from the real file bytes
    // (mirroring service::deploy_transform), so this has to happen in two
    // steps: load once (untrusted, just to learn the hash - allowed to
    // fail exactly like the test above, that's not what's under test
    // here), configure trust for that hash, then load again for real.
    EXPECT_THROW(load_wasm("shared-memory.wasm"), wasm::wasm_exception);
    auto sha256 = meta().binary_sha256;
    ASSERT_FALSE(sha256.empty());

    config::shard_local_cfg().wasm_trusted_modules.set_value(
      std::vector<config::wasm_trusted_module>{config::wasm_trusted_module{
        .sha256_hex = sha256,
        .capabilities = {config::wasm_capability::shared_memory},
        .allowed_targets = {},
      }});
    load_wasm("shared-memory.wasm");

    // The guest already registered its region during its own startup, in
    // main() - before this test ever calls transform() - which is the
    // whole point: delivering data into the region needs no guest ABI call
    // at all, only registering where it goes, once, needs one.
    const ss::sstring message = "hello from the host, no abi call needed";
    bytes_view data(
      // NOLINTNEXTLINE(*-reinterpret-*)
      reinterpret_cast<const uint8_t*>(message.data()),
      message.size());
    ASSERT_TRUE(engine()->write_shared_memory(data));

    auto batch = make_tiny_batch();
    auto result = transform(batch);
    const auto& result_records = result.copy_records();
    ASSERT_EQ(result_records.size(), 1);
    auto value = result_records.front().value().linearize_to_string();
    ASSERT_EQ(value, message);
}
