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

#include "config/wasm_trusted_module.h"
#include "json/stringbuffer.h"
#include "json/writer.h"

#include <seastar/testing/thread_test_case.hh>

#include <yaml-cpp/yaml.h>

// wasm_trusted_modules is the property that decides which wasm binaries get
// out of the sandbox, so its parsing is a security boundary and gets the same
// treatment every other custom config type here gets.
//
// The round trip in particular is not optional: cluster_config_test.py's
// test_valid_settings sets EVERY property in the schema to a non-default value
// and checks it comes back unchanged, explicitly to catch types whose output
// format does not match their input. Without a unit test, a break in that
// round trip surfaces as a confusing bulk-ducktape failure instead of here.

namespace {
constexpr std::string_view valid_sha
  = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

config::wasm_trusted_module net_entry() {
    return {
      .sha256_hex = ss::sstring(valid_sha),
      .capabilities = {config::wasm_capability::network},
      .allowed_targets = {net::unresolved_address("refdata.internal", 443)}};
}
} // namespace

SEASTAR_THREAD_TEST_CASE(capability_string_round_trip) {
    using cap = config::wasm_capability;
    for (auto c : {cap::network, cap::shared_memory, cap::relay_consumer}) {
        auto parsed = config::from_string_view<cap>(config::to_string_view(c));
        BOOST_REQUIRE(parsed.has_value());
        BOOST_CHECK(*parsed == c);
    }
    // An unknown capability must not silently become a real one - this is the
    // difference between rejecting a typo and granting the wrong privilege.
    BOOST_CHECK(!config::from_string_view<cap>("network ").has_value());
    BOOST_CHECK(!config::from_string_view<cap>("NETWORK").has_value());
    BOOST_CHECK(!config::from_string_view<cap>("filesystem").has_value());
    BOOST_CHECK(!config::from_string_view<cap>("").has_value());
}

SEASTAR_THREAD_TEST_CASE(yaml_round_trip_preserves_every_field) {
    auto orig = net_entry();
    orig.capabilities.push_back(config::wasm_capability::shared_memory);
    orig.allowed_targets.emplace_back("other.internal", 8443);

    auto node = YAML::convert<config::wasm_trusted_module>::encode(orig);
    config::wasm_trusted_module decoded;
    BOOST_REQUIRE(
      YAML::convert<config::wasm_trusted_module>::decode(node, decoded));
    // Whole-struct equality, not field spot-checks: a field added later
    // without encode/decode support has to fail this.
    BOOST_CHECK(decoded == orig);
}

SEASTAR_THREAD_TEST_CASE(json_serialization_emits_the_documented_shape) {
    json::StringBuffer buf;
    json::Writer<json::StringBuffer> w{buf};
    json::rjson_serialize(w, net_entry());
    std::string out{buf.GetString()};
    // The admin API and the property's own `example` advertise these keys, so
    // renaming one silently breaks anything reading the config back.
    BOOST_CHECK(out.find("\"sha256\"") != std::string::npos);
    BOOST_CHECK(out.find("\"capabilities\"") != std::string::npos);
    BOOST_CHECK(out.find("\"network\"") != std::string::npos);
    BOOST_CHECK(out.find("\"allowed_targets\"") != std::string::npos);
    BOOST_CHECK(out.find("refdata.internal") != std::string::npos);
}

SEASTAR_THREAD_TEST_CASE(validate_accepts_a_well_formed_entry) {
    BOOST_CHECK(net_entry().validate().empty());

    config::wasm_trusted_module shm{
      .sha256_hex = ss::sstring(valid_sha),
      .capabilities = {config::wasm_capability::shared_memory}};
    BOOST_CHECK(shm.validate().empty());
}

SEASTAR_THREAD_TEST_CASE(validate_rejects_malformed_digests) {
    auto bad_sha = [](std::string_view sha) {
        config::wasm_trusted_module m{
          .sha256_hex = ss::sstring(sha),
          .capabilities = {config::wasm_capability::shared_memory}};
        return !m.validate().empty();
    };
    BOOST_CHECK(bad_sha(""));
    BOOST_CHECK(bad_sha(valid_sha.substr(0, 63)));      // too short
    BOOST_CHECK(bad_sha(std::string(valid_sha) + "0")); // too long
    // Uppercase is rejected on purpose: trust is matched against a digest
    // rendered lowercase, so accepting uppercase here would create entries
    // that validate and then never match anything.
    BOOST_CHECK(bad_sha(
      "0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef"));
    BOOST_CHECK(bad_sha(
      "0123456789abcdeg0123456789abcdef0123456789abcdef0123456789abcdef"));
}

SEASTAR_THREAD_TEST_CASE(validate_rejects_grants_that_cannot_work) {
    // Each of these parses fine and grants nothing usable. Silently accepting
    // one leaves an operator believing a guest has a privilege it does not.
    config::wasm_trusted_module no_caps{.sha256_hex = ss::sstring(valid_sha)};
    BOOST_CHECK(!no_caps.validate().empty());

    auto net_no_targets = net_entry();
    net_no_targets.allowed_targets.clear();
    BOOST_CHECK(!net_no_targets.validate().empty());

    // relay_consumer delivers through the shared-memory region, so without
    // shared_memory there is nowhere to deliver to.
    config::wasm_trusted_module relay_only{
      .sha256_hex = ss::sstring(valid_sha),
      .capabilities = {config::wasm_capability::relay_consumer}};
    BOOST_CHECK(!relay_only.validate().empty());

    config::wasm_trusted_module relay_with_shm{
      .sha256_hex = ss::sstring(valid_sha),
      .capabilities = {
        config::wasm_capability::relay_consumer,
        config::wasm_capability::shared_memory}};
    BOOST_CHECK(relay_with_shm.validate().empty());
}

SEASTAR_THREAD_TEST_CASE(has_capability_does_not_grant_by_adjacency) {
    auto m = net_entry();
    BOOST_CHECK(m.has_capability(config::wasm_capability::network));
    // Holding one capability must not imply another.
    BOOST_CHECK(!m.has_capability(config::wasm_capability::shared_memory));
    BOOST_CHECK(!m.has_capability(config::wasm_capability::relay_consumer));
}
