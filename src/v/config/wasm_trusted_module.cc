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

#include <algorithm>
#include <cstdlib>

namespace config {

std::string_view to_string_view(wasm_capability c) {
    switch (c) {
    case wasm_capability::network:
        return "network";
    case wasm_capability::shared_memory:
        return "shared_memory";
    case wasm_capability::relay_consumer:
        return "relay_consumer";
    }
}

fmt::iterator format_to(wasm_capability c, fmt::iterator it) {
    return fmt::format_to(it, "{}", to_string_view(c));
}

template<>
std::optional<wasm_capability>
from_string_view<wasm_capability>(std::string_view sv) {
    if (sv == "network") {
        return wasm_capability::network;
    }
    if (sv == "shared_memory") {
        return wasm_capability::shared_memory;
    }
    if (sv == "relay_consumer") {
        return wasm_capability::relay_consumer;
    }
    return std::nullopt;
}

namespace {
bool is_sha256_hex(std::string_view sv) {
    return sv.size() == 64 && std::ranges::all_of(sv, [](char ch) {
               return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
           });
}
} // namespace

fmt::iterator wasm_trusted_module::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "{{sha256: {}, capabilities: [", sha256_hex);
    for (size_t i = 0; i < capabilities.size(); ++i) {
        if (i > 0) {
            it = fmt::format_to(it, ", ");
        }
        it = fmt::format_to(it, "{}", to_string_view(capabilities[i]));
    }
    return fmt::format_to(
      it, "], allowed_targets: [{}]}}", fmt::join(allowed_targets, ", "));
}

ss::sstring wasm_trusted_module::validate() const {
    if (!is_sha256_hex(sha256_hex)) {
        return ss::format(
          "sha256 must be exactly 64 lowercase hex characters, got '{}'",
          sha256_hex);
    }
    if (capabilities.empty()) {
        return "capabilities must not be empty - an entry granting nothing "
               "is not meaningful, remove it instead";
    }
    if (has_capability(wasm_capability::network) && allowed_targets.empty()) {
        return "the 'network' capability requires at least one entry in "
               "allowed_targets - granting network with no targets is not "
               "meaningful, either add targets or remove the capability";
    }
    return {};
}

bool wasm_trusted_module::has_capability(wasm_capability c) const {
    return std::ranges::find(capabilities, c) != capabilities.end();
}

} // namespace config

namespace YAML {

Node convert<config::wasm_trusted_module>::encode(const type& rhs) {
    Node node;
    node["sha256"] = rhs.sha256_hex;
    Node caps(NodeType::Sequence);
    for (auto c : rhs.capabilities) {
        caps.push_back(ss::sstring(config::to_string_view(c)));
    }
    node["capabilities"] = caps;
    Node targets(NodeType::Sequence);
    for (const auto& t : rhs.allowed_targets) {
        Node target;
        target["address"] = t.host();
        target["port"] = t.port();
        targets.push_back(target);
    }
    node["allowed_targets"] = targets;
    return node;
}

bool convert<config::wasm_trusted_module>::decode(const Node& node, type& rhs) {
    if (!node["sha256"]) {
        return false;
    }
    rhs.sha256_hex = node["sha256"].as<ss::sstring>();
    rhs.capabilities.clear();
    if (auto caps = node["capabilities"]; bool(caps)) {
        for (const auto& c : caps) {
            auto parsed = config::from_string_view<config::wasm_capability>(
              c.as<ss::sstring>());
            if (!parsed) {
                return false;
            }
            rhs.capabilities.push_back(*parsed);
        }
    }
    rhs.allowed_targets.clear();
    if (auto targets = node["allowed_targets"]; bool(targets)) {
        for (const auto& t : targets) {
            if (!t["address"] || !t["port"]) {
                return false;
            }
            rhs.allowed_targets.emplace_back(
              t["address"].as<ss::sstring>(), t["port"].as<uint16_t>());
        }
    }
    return true;
}

} // namespace YAML

void json::rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::wasm_trusted_module& m) {
    w.StartObject();
    w.Key("sha256");
    w.String(m.sha256_hex);
    w.Key("capabilities");
    w.StartArray();
    for (auto c : m.capabilities) {
        auto sv = config::to_string_view(c);
        w.String(sv.data(), sv.length());
    }
    w.EndArray();
    w.Key("allowed_targets");
    w.StartArray();
    for (const auto& t : m.allowed_targets) {
        w.StartObject();
        w.Key("address");
        w.String(t.host());
        w.Key("port");
        w.Uint(t.port());
        w.EndObject();
    }
    w.EndArray();
    w.EndObject();
}
