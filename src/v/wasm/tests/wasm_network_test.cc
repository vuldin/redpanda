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
#include "utils/unresolved_address.h"
#include "wasm/errc.h"
#include "wasm/tests/wasm_fixture.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>

#include <chrono>

namespace {
using namespace std::chrono_literals;

// A fixed, pre-chosen port for this test binary only, following the same
// convention http::tests::http_imposter_fixture documents (each unit test
// binary picks its own constant, since listening on TCP from a unit test
// means avoiding collisions with other test binaries rather than using
// ephemeral+readback).
constexpr uint16_t test_server_port = 28471;

ss::socket_address test_server_address() {
    return {ss::net::inet_address("127.0.0.1"), test_server_port};
}

// Accepts exactly one connection, reads exactly request.size() bytes (this
// is a test, so the request length is known up front - a real protocol
// would need its own framing to know where a request ends on a stream
// with no message boundaries), writes response, then closes - which is
// what lets bulk_load's/recv's read-until-peer-closes loop terminate.
ss::future<>
run_one_shot_server(ss::server_socket server, bytes request, bytes response) {
    auto ar = co_await server.accept();
    auto conn = std::move(ar.connection);
    auto in = conn.input();
    auto out = conn.output();
    auto got = co_await in.read_exactly(request.size());
    EXPECT_EQ(
      std::string_view(got.get(), got.size()),
      std::string_view(
        // NOLINTNEXTLINE(*-reinterpret-*)
        reinterpret_cast<const char*>(request.data()),
        request.size()));
    // NOLINTNEXTLINE(*-reinterpret-*)
    co_await out.write(
      reinterpret_cast<const char*>(response.data()), response.size());
    co_await out.flush();
    co_await out.close();
    co_await in.close();
}

// Accepts exactly one connection and then deliberately never reads or
// writes anything on it - a relay that's up (the connection stays
// ESTABLISHED at the TCP level) but never draining, which is exactly the
// scenario the buffered-push fix (network_module::send() /
// drain_pending_pushes()) exists for. Runs until `as` is aborted, so the
// test controls when this finishes rather than this hanging forever.
ss::future<>
run_unresponsive_server(ss::server_socket server, ss::abort_source& as) {
    auto ar = co_await server.accept();
    auto conn = std::move(ar.connection);
    try {
        co_await ss::sleep_abortable(24h, as);
    } catch (const ss::sleep_aborted&) {
        // Expected - the test is done asserting and told us to stop.
    }
    conn.shutdown_input();
    conn.shutdown_output();
}

} // namespace

TEST_F(WasmTestFixture, NetworkUntrustedBinaryFailsToInstantiate) {
    config::shard_local_cfg().wasm_trusted_modules.set_value(
      std::vector<config::wasm_trusted_module>{});
    EXPECT_THROW(load_wasm("network-echo.wasm"), wasm::wasm_exception);
}

TEST_F(WasmTestFixture, NetworkTrustedBinaryConnectsSendsAndReceives) {
    ss::listen_options lo;
    lo.reuse_address = true;
    auto server = ss::engine().listen(test_server_address(), lo);
    const bytes request = bytes::from_string("ping from the guest");
    const bytes response = bytes::from_string("pong from the test server");
    auto server_task = run_one_shot_server(
      std::move(server), request, response);

    EXPECT_THROW(load_wasm("network-echo.wasm"), wasm::wasm_exception);
    auto sha256 = meta().binary_sha256;
    ASSERT_FALSE(sha256.empty());

    config::shard_local_cfg().wasm_trusted_modules.set_value(
      std::vector<config::wasm_trusted_module>{config::wasm_trusted_module{
        .sha256_hex = sha256,
        .capabilities = {config::wasm_capability::network},
        .allowed_targets = {net::unresolved_address(
          "127.0.0.1", test_server_port)},
      }});
    load_wasm("network-echo.wasm");
    std::move(server_task).get();

    auto batch = make_tiny_batch();
    auto result = transform(batch);
    const auto& result_records = result.copy_records();
    ASSERT_EQ(result_records.size(), 1);
    auto value = result_records.front().value().linearize_to_string();
    ASSERT_EQ(value, "pong from the test server");
}

TEST_F(WasmTestFixture, BulkLoadDeliversFullResponseViaSharedMemory) {
    ss::listen_options lo;
    lo.reuse_address = true;
    auto server = ss::engine().listen(test_server_address(), lo);
    const bytes request = bytes::from_string("give me the snapshot");
    const bytes response = bytes::from_string(
      "this is the whole snapshot, delivered in one bulk_load call");
    auto server_task = run_one_shot_server(
      std::move(server), request, response);

    EXPECT_THROW(load_wasm("bulk-load.wasm"), wasm::wasm_exception);
    auto sha256 = meta().binary_sha256;
    ASSERT_FALSE(sha256.empty());

    config::shard_local_cfg().wasm_trusted_modules.set_value(
      std::vector<config::wasm_trusted_module>{config::wasm_trusted_module{
        .sha256_hex = sha256,
        .capabilities
        = {config::wasm_capability::network, config::wasm_capability::shared_memory},
        .allowed_targets = {net::unresolved_address(
          "127.0.0.1", test_server_port)},
      }});
    // bulk_load runs during the guest's own main(), before load_wasm's
    // _engine->start().get() returns - the server task above has to make
    // progress concurrently with that wait, which it does because both are
    // ordinary seastar futures on the same reactor.
    load_wasm("bulk-load.wasm");
    std::move(server_task).get();

    auto batch = make_tiny_batch();
    auto result = transform(batch);
    const auto& result_records = result.copy_records();
    ASSERT_EQ(result_records.size(), 1);
    auto value = result_records.front().value().linearize_to_string();
    ASSERT_EQ(
      value, "this is the whole snapshot, delivered in one bulk_load call");
}

// Regression test for the buffered/non-blocking push fix: send() must
// enqueue and return immediately regardless of the peer, and the engine's
// per-batch drain of those pushes must never block the batch-processing
// loop that called it - against a peer that accepts the connection and
// then never reads anything at all, 10000 send() calls plus one batch's
// worth of drain must still complete in a small fraction of a second.
// Before this fix, network_module::send() was co_await write()+flush()
// directly on the guest's call - this same scenario would have hung the
// whole test (and, in production, the whole wasm engine instance)
// instead.
TEST_F(WasmTestFixture, PushToUnresponsivePeerDoesNotBlock) {
    ss::listen_options lo;
    lo.reuse_address = true;
    auto server = ss::engine().listen(test_server_address(), lo);
    ss::abort_source server_as;
    auto server_task = run_unresponsive_server(std::move(server), server_as);

    EXPECT_THROW(load_wasm("network-slow-push.wasm"), wasm::wasm_exception);
    auto sha256 = meta().binary_sha256;
    ASSERT_FALSE(sha256.empty());

    config::shard_local_cfg().wasm_trusted_modules.set_value(
      std::vector<config::wasm_trusted_module>{config::wasm_trusted_module{
        .sha256_hex = sha256,
        .capabilities = {config::wasm_capability::network},
        .allowed_targets = {net::unresolved_address(
          "127.0.0.1", test_server_port)},
      }});
    load_wasm("network-slow-push.wasm");

    auto batch = make_tiny_batch();
    auto start = ss::lowres_clock::now();
    auto result = transform(batch);
    auto elapsed = ss::lowres_clock::now() - start;

    server_as.request_abort();
    std::move(server_task).get();

    // The actual property under test. This would be single-digit
    // milliseconds in practice - a generous bound to keep this robust
    // under CI load without weakening what it actually proves (the old,
    // unbuffered send() would have blocked for the OS-level TCP send
    // timeout, which is on the order of minutes, not seconds).
    EXPECT_LT(elapsed, 5s);

    const auto& result_records = result.copy_records();
    ASSERT_EQ(result_records.size(), 1);
    auto value = result_records.front().value().linearize_to_string();
    int success_count = std::stoi(value);
    // At least some pushes succeeded - not an exact count, since that
    // would couple this test to network_module's internal buffer-size
    // constant rather than to the behavior actually being tested here.
    EXPECT_GT(success_count, 0);
}
