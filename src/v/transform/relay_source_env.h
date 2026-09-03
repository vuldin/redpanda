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

#include "model/transform.h"

#include <seastar/core/smp.hh>

#include <charconv>
#include <optional>
#include <string_view>

namespace transform {

// A transform is relay-sourced - fed by the relay (src/v/relay/) instead of
// reading its input partition - when it sets this env var at deploy time.
// This is the v1 opt-in mechanism for in-broker relay consumers; a
// first-class metadata field is the planned follow-up. (Not REDPANDA_-
// prefixed: plugin_frontend rejects reserved REDPANDA_* env keys.)
inline constexpr std::string_view relay_source_env_var = "RELAY_SOURCE";

inline bool is_relay_sourced(const model::transform_metadata& meta) {
    auto it = meta.environment.find(ss::sstring(relay_source_env_var));
    if (it == meta.environment.end()) {
        return false;
    }
    const auto& v = it->second;
    return v == "1" || v == "true" || v == "yes";
}

// Pins a relay-sourced transform onto a specific shard on every node that
// deploys it, bypassing the normal leadership-derived shard assignment (see
// transform_manager.cc's hint-placement handling) - a v1 prototype
// mechanism for spreading relay consumers across cores instead of piling
// them onto whichever shard leads the input topic. Cluster-wide metadata
// means this pins shard N *on every node* that has the transform deployed,
// not a single cluster-wide pin. Only meaningful when is_relay_sourced() is
// also true - callers are responsible for checking that separately.
inline constexpr std::string_view relay_target_shard_env_var
  = "RELAY_TARGET_SHARD";

inline std::optional<ss::shard_id>
relay_target_shard(const model::transform_metadata& meta) {
    auto it = meta.environment.find(ss::sstring(relay_target_shard_env_var));
    if (it == meta.environment.end()) {
        return std::nullopt;
    }
    const auto& v = it->second;
    unsigned parsed = 0;
    auto res = std::from_chars(v.data(), v.data() + v.size(), parsed);
    if (res.ec != std::errc{} || res.ptr != v.data() + v.size()) {
        return std::nullopt;
    }
    return ss::shard_id(parsed);
}

} // namespace transform
