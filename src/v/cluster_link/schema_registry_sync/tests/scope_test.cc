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

} // namespace cluster_link::tests
