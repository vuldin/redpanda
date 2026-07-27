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

#include "base/format_to.h"
#include "base/seastarx.h"
#include "config/from_string_view.h"
#include "config/property.h"
#include "json/_include_first.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "utils/unresolved_address.h"

#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include <yaml-cpp/node/node.h>

#include <algorithm>
#include <iterator>
#include <optional>
#include <string_view>
#include <vector>

namespace config {

// The fixed set of elevated capabilities a specific, admin-vetted wasm
// binary can be granted. Deliberately small and closed - this is not a
// generic permission bitset, it is the exact list of things the wasm host
// modules in src/v/wasm/ know how to gate.
enum class wasm_capability {
    // Real outbound network connections via src/v/net/, restricted to the
    // entry's own allowed_targets. Does not make the sock_* WASI stubs
    // functional - this is a separate, purpose-built host module.
    network,
    // Direct read access to a host-owned memory region mapped into the
    // guest's own linear memory, bypassing the normal per-call ABI.
    shared_memory,
};

std::string_view to_string_view(wasm_capability c);
fmt::iterator format_to(wasm_capability c, fmt::iterator it);

template<>
std::optional<wasm_capability>
from_string_view<wasm_capability>(std::string_view sv);

// One entry in the wasm_trusted_modules cluster property: grants a specific,
// content-identified wasm binary a set of elevated capabilities beyond the
// default sandbox every other transform runs under.
//
// Trust is keyed on the binary's own SHA-256 digest (see
// model::transform_metadata::binary_sha256, computed once at deploy time),
// not on the transform's name - redeploying different code under an
// already-trusted transform name does not inherit these capabilities, since
// the new binary's digest won't match any entry here.
struct wasm_trusted_module {
    friend bool operator==(
      const wasm_trusted_module&, const wasm_trusted_module&) = default;

    fmt::iterator format_to(fmt::iterator it) const;

    // Returns a human-readable validation error, or an empty string if this
    // entry is well-formed.
    ss::sstring validate() const;

    bool has_capability(wasm_capability c) const;

    // Lowercase hex-encoded SHA-256 digest of the exact wasm binary this
    // entry applies to (64 characters).
    ss::sstring sha256_hex;
    std::vector<wasm_capability> capabilities;
    // Only meaningful when `capabilities` contains `network`. Granting
    // `network` with no entries here grants no usable connectivity - the
    // allowlist is closed by default, both at this level and at the
    // capability level.
    std::vector<net::unresolved_address> allowed_targets;
};

template<>
consteval std::string_view detail::property_type_name<wasm_trusted_module>() {
    return "config::wasm_trusted_module";
}

template<class InputIt>
std::optional<ss::sstring>
validate_wasm_trusted_modules(const InputIt first, const InputIt last) {
    for (auto i = first; i != last; ++i) {
        if (auto verr = i->validate(); !verr.empty()) {
            return ss::format(
              "Validation failed for wasm_trusted_modules entry #{}: {}",
              std::distance(first, i),
              verr);
        }
        if (std::any_of(first, i, [i](const wasm_trusted_module& m) {
                return m.sha256_hex == i->sha256_hex;
            })) {
            return ss::format(
              "Duplicate wasm_trusted_modules entry for sha256 {}",
              i->sha256_hex);
        }
    }
    return std::nullopt;
}

} // namespace config

namespace YAML {

template<>
struct convert<config::wasm_trusted_module> {
    using type = config::wasm_trusted_module;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::wasm_trusted_module& m);

} // namespace json
