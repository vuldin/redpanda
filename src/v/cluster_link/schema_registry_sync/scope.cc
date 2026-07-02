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

#include "cluster_link/schema_registry_sync/scope.h"

#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

namespace cluster_link::schema_registry_sync {

ss::noncopyable_function<bool(const ppsr::context_subject&)> make_in_scope(
  chunked_hash_set<ppsr::context> contexts,
  chunked_hash_set<ppsr::context_subject> subjects) {
    const bool unfiltered = contexts.empty() && subjects.empty();
    return [unfiltered,
            contexts = std::move(contexts),
            subjects = std::move(subjects)](const ppsr::context_subject& sub) {
        return unfiltered || contexts.contains(sub.ctx)
               || subjects.contains(sub);
    };
}

std::optional<ss::sstring> check_preconditions(
  const model::schema_registry_sync_config& config,
  const chunked_hash_set<ppsr::context>& in_scope_contexts,
  bool qualified_subjects_enabled) {
    if (qualified_subjects_enabled) {
        return std::nullopt;
    }

    // With qualified subjects off the destination can only hold the default
    // context, so reject any non-default context that would be written there.
    auto rejected = [](const ppsr::context& ctx) -> std::optional<ss::sstring> {
        if (ctx == ppsr::default_context) {
            return std::nullopt;
        }
        return ssx::sformat(
          "non-default context '{}' requires "
          "schema_registry_enable_qualified_subjects to be enabled",
          ctx);
    };

    const auto* api = config.api_mode();
    if (api == nullptr) {
        return std::nullopt;
    }

    // Identity mapping (also the default when no remapping is configured):
    // every context the run touches becomes a destination context -- discovered
    // from the source, or named by a context or subject filter the source may
    // not yet hold.
    auto reject_identity = [&]() -> std::optional<ss::sstring> {
        for (const auto& ctx : in_scope_contexts) {
            if (auto r = rejected(ctx)) {
                return r;
            }
        }
        // The filter may name a context the source does not hold yet; under
        // identity mapping it would still be written verbatim, so fail fast.
        for (const auto& ctx : api->filter.contexts) {
            if (auto r = rejected(ppsr::context{ctx})) {
                return r;
            }
        }
        // Parse as qualified to catch a non-default context the operator
        // expressed in subject syntax; under the off flag it would otherwise be
        // silently flattened to a literal default-context subject.
        for (const auto& sub : api->filter.subjects) {
            auto parsed = ppsr::context_subject::from_string(
              sub, ppsr::qualified_subjects_enabled::yes);
            if (auto r = rejected(parsed.ctx)) {
                return r;
            }
        }
        return std::nullopt;
    };
    if (!api->destination.has_value()) {
        return reject_identity();
    }
    using identity_context_mapping
      = model::schema_registry_sync_config::identity_context_mapping;
    using exact_context_mapping
      = model::schema_registry_sync_config::exact_context_mapping;
    return ss::visit(
      *api->destination,
      [&](const identity_context_mapping&) { return reject_identity(); },
      [&](const exact_context_mapping& m) -> std::optional<ss::sstring> {
          // Remapping writes to the mapping targets; source contexts are
          // remapped away, so only the targets matter.
          for (const auto& [_, dest] : m.mappings) {
              if (auto r = rejected(ppsr::context{dest})) {
                  return r;
              }
          }
          return std::nullopt;
      });
}

} // namespace cluster_link::schema_registry_sync
