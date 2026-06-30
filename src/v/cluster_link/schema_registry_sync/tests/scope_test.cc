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
#include "pandaproxy/schema_registry/types.h"
#include "test_utils/test.h"

namespace cluster_link::tests {

namespace ppsr = pandaproxy::schema_registry;
namespace srs = cluster_link::schema_registry_sync;

namespace {
model::schema_registry_sync_config config_with(
  model::schema_registry_sync_config::shadow_schema_registry_api api = {}) {
    model::schema_registry_sync_config config;
    config.sync_mode = std::move(api);
    return config;
}
} // namespace

TEST(scope, in_scope_predicate_matches_configured_contexts) {
    chunked_hash_set<ppsr::context> contexts{
      ppsr::default_context, ppsr::context{".b"}};
    // Empty subject set: no per-subject restriction within the in-scope
    // contexts.
    auto in_scope = srs::make_in_scope(std::move(contexts), {});

    EXPECT_TRUE(in_scope(ppsr::context_subject::unqualified("orders")));
    EXPECT_TRUE(
      in_scope(ppsr::context_subject{ppsr::context{".b"}, ppsr::subject{"x"}}));
    EXPECT_FALSE(
      in_scope(ppsr::context_subject{ppsr::context{".c"}, ppsr::subject{"x"}}));
}

TEST(scope, in_scope_filter_union_semantics) {
    // The source filter's context and subject selectors union: contexts select
    // whole contexts, subjects add individual context-qualified subjects, and
    // an empty filter replicates everything. Every scenario checks the same
    // four subjects in the same order, so the cases read as a truth table:
    //   * default_a:     subject "a" in the default context
    //   * other_x:       subject "x" in the .other context
    //   * example:       subject "example" in the .onemore context
    //   * onemore_other: subject "other" in the .onemore context (the rest of
    //                    .onemore that an individual `example` selector leaves
    //                    out)
    const auto other = ppsr::context{".other"};
    const auto onemore = ppsr::context{".onemore"};
    const auto default_a = ppsr::context_subject::unqualified("a");
    const auto other_x = ppsr::context_subject{other, ppsr::subject{"x"}};
    const auto example = ppsr::context_subject{
      onemore, ppsr::subject{"example"}};
    const auto onemore_other = ppsr::context_subject{
      onemore, ppsr::subject{"other"}};

    auto ctxs = [](std::initializer_list<ppsr::context> cs) {
        chunked_hash_set<ppsr::context> out;
        for (const auto& c : cs) {
            out.insert(c);
        }
        return out;
    };
    auto subs = [](std::initializer_list<ppsr::context_subject> ss) {
        chunked_hash_set<ppsr::context_subject> out;
        for (const auto& s : ss) {
            out.insert(s);
        }
        return out;
    };

    // No filter: every subject is in scope.
    {
        auto in_scope = srs::make_in_scope(ctxs({}), subs({}));
        EXPECT_TRUE(in_scope(default_a));
        EXPECT_TRUE(in_scope(other_x));
        EXPECT_TRUE(in_scope(example));
        EXPECT_TRUE(in_scope(onemore_other));
    }
    // Context selector only: the whole .other context, nothing else.
    {
        auto in_scope = srs::make_in_scope(ctxs({other}), subs({}));
        EXPECT_FALSE(in_scope(default_a));
        EXPECT_TRUE(in_scope(other_x));
        EXPECT_FALSE(in_scope(example));
        EXPECT_FALSE(in_scope(onemore_other));
    }
    // Context UNION subject: all of .other, plus the single .onemore:example;
    // the rest of .onemore stays out.
    {
        auto in_scope = srs::make_in_scope(ctxs({other}), subs({example}));
        EXPECT_FALSE(in_scope(default_a));
        EXPECT_TRUE(in_scope(other_x));
        EXPECT_TRUE(in_scope(example));
        EXPECT_FALSE(in_scope(onemore_other));
    }
    // Subject selector only: just the single .onemore:example subject.
    {
        auto in_scope = srs::make_in_scope(ctxs({}), subs({example}));
        EXPECT_FALSE(in_scope(default_a));
        EXPECT_FALSE(in_scope(other_x));
        EXPECT_TRUE(in_scope(example));
        EXPECT_FALSE(in_scope(onemore_other));
    }
}

TEST(scope, preconditions_remapping_target_requires_qualified) {
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".", ".prod"); // default -> non-default target
    api.destination = std::move(mapping);
    auto config = config_with(std::move(api));

    chunked_hash_set<ppsr::context> in_scope;
    in_scope.insert(ppsr::default_context);
    // Flag off rejects the non-default target; flag on allows any context.
    EXPECT_TRUE(srs::check_preconditions(config, in_scope, false).has_value());
    EXPECT_FALSE(srs::check_preconditions(config, in_scope, true).has_value());
}

TEST(scope, preconditions_remapping_to_default_allowed) {
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".src", "."); // non-default source -> default
    api.destination = std::move(mapping);
    auto config = config_with(std::move(api));

    // The non-default source context (in_scope) is irrelevant under remapping:
    // only the default target is written.
    chunked_hash_set<ppsr::context> in_scope;
    in_scope.insert(ppsr::context{".src"});
    EXPECT_FALSE(srs::check_preconditions(config, in_scope, false).has_value());
}

TEST(scope, preconditions_nondefault_filter_requires_qualified) {
    chunked_hash_set<ppsr::context> empty;

    // A non-default context filter faults eagerly, before the source is known
    // to hold the context.
    {
        model::schema_registry_sync_config::shadow_schema_registry_api api;
        api.filter.contexts.push_back(".prod");
        auto config = config_with(std::move(api));
        EXPECT_TRUE(srs::check_preconditions(config, empty, false).has_value());
        EXPECT_FALSE(srs::check_preconditions(config, empty, true).has_value());
    }
    // A subject filter in qualified syntax names a non-default context; it is
    // parsed as qualified so the off flag cannot flatten it to a default
    // subject and hide the fault.
    {
        model::schema_registry_sync_config::shadow_schema_registry_api api;
        api.filter.subjects.push_back(":.prod:orders");
        auto config = config_with(std::move(api));
        EXPECT_TRUE(srs::check_preconditions(config, empty, false).has_value());
        EXPECT_FALSE(srs::check_preconditions(config, empty, true).has_value());
    }
}

TEST(scope, preconditions_explicit_identity_matches_no_destination) {
    // An explicit identity mapping is the default (no-destination) case, so a
    // non-default filter context must fault identically -- the two config
    // spellings cannot diverge.
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    api.filter.contexts.push_back(".prod");
    api.destination
      = model::schema_registry_sync_config::identity_context_mapping{};
    auto config = config_with(std::move(api));

    chunked_hash_set<ppsr::context> empty;
    EXPECT_TRUE(srs::check_preconditions(config, empty, false).has_value());
    EXPECT_FALSE(srs::check_preconditions(config, empty, true).has_value());
}

TEST(scope, preconditions_remapping_ignores_source_filter) {
    // Mapping a non-default source context onto the default destination is
    // allowed even with the flag off: the source-filter name is remapped away,
    // so only the (default) target is written.
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    api.filter.contexts.push_back(".prod");
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".prod", "."); // non-default source -> default
    api.destination = std::move(mapping);
    auto config = config_with(std::move(api));

    chunked_hash_set<ppsr::context> in_scope;
    in_scope.insert(ppsr::context{".prod"});
    EXPECT_FALSE(srs::check_preconditions(config, in_scope, false).has_value());
}

TEST(scope, preconditions_require_qualified_subjects_for_nondefault) {
    auto config = config_with();

    chunked_hash_set<ppsr::context> nondefault;
    nondefault.insert(ppsr::context{".b"});
    EXPECT_TRUE(
      srs::check_preconditions(config, nondefault, false).has_value());
    EXPECT_FALSE(
      srs::check_preconditions(config, nondefault, true).has_value());

    chunked_hash_set<ppsr::context> default_only;
    default_only.insert(ppsr::default_context);
    EXPECT_FALSE(
      srs::check_preconditions(config, default_only, false).has_value());
}

} // namespace cluster_link::tests
