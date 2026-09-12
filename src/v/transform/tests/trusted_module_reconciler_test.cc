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

#include "config/configuration.h"
#include "config/wasm_trusted_module.h"
#include "model/transform.h"
#include "test_utils/async.h"
#include "test_utils/test.h"
#include "transform/trusted_module_reconciler.h"

#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <vector>

// The wiring these cover used to live in transform::service, where nothing
// could reach it: the service needs a dozen sharded dependencies to build and
// no test in the tree constructs one, so "the watch is installed and every
// affected transform is rebuilt" was verifiable only by reading the code.
//
// That matters because the failure is silent and security-relevant. If the
// watch is never installed, or the dispatch stops after the first transform,
// an operator who removes a binary from wasm_trusted_modules sees the property
// change and believes the capability is revoked, while the module keeps using
// it until something unrelated restarts the processor.

namespace transform {
namespace {

constexpr std::string_view sha_a
  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr std::string_view sha_b
  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

config::wasm_trusted_module entry(std::string_view sha) {
    return {
      .sha256_hex = ss::sstring(sha),
      .capabilities = {config::wasm_capability::shared_memory}};
}

model::transform_metadata meta_with(std::string_view sha) {
    model::transform_metadata m;
    m.name = model::transform_name{
      ss::sstring("xform-") + ss::sstring(sha.substr(0, 4))};
    m.binary_sha256 = ss::sstring(sha);
    return m;
}

class ReconcilerTest : public ::testing::Test {
public:
    void SetUp() override {
        config::shard_local_cfg().wasm_trusted_modules.set_value(
          std::vector<config::wasm_trusted_module>{});
    }
    void TearDown() override {
        if (_r) {
            _r->stop().get();
            _r.reset();
        }
        config::shard_local_cfg().wasm_trusted_modules.set_value(
          std::vector<config::wasm_trusted_module>{});
    }

    // Built after the transform map is arranged, so the reconciler's initial
    // snapshot matches what the "engines" were started with.
    void make_reconciler() {
        _r = std::make_unique<trusted_module_reconciler>(
          [this] { return _transforms; },
          [this](const model::transform_metadata& meta) {
              _invalidated.push_back(meta.binary_sha256);
          },
          [this](model::transform_id id) {
              _rebuilt.push_back(id);
              return ss::now();
          });
    }

    void add_transform(model::transform_id id, std::string_view sha) {
        _transforms.emplace(id, meta_with(sha));
    }
    void add_transform_without_digest(model::transform_id id) {
        model::transform_metadata m;
        m.name = model::transform_name{"no-digest"};
        _transforms.emplace(id, m);
    }
    void set_allowlist(std::vector<config::wasm_trusted_module> v) {
        config::shard_local_cfg().wasm_trusted_modules.set_value(std::move(v));
    }

    trusted_module_reconciler& reconciler() { return *_r; }
    const std::vector<model::transform_id>& rebuilt() const { return _rebuilt; }
    const std::vector<ss::sstring>& invalidated() const { return _invalidated; }

private:
    trusted_module_reconciler::transform_map _transforms;
    std::vector<model::transform_id> _rebuilt;
    std::vector<ss::sstring> _invalidated;
    std::unique_ptr<trusted_module_reconciler> _r;
};

} // namespace

TEST_F(ReconcilerTest, RevocationRebuildsTheAffectedTransform) {
    set_allowlist({entry(sha_a)});
    add_transform(model::transform_id(1), sha_a);
    make_reconciler();

    // Withdraw the grant.
    set_allowlist({});
    reconciler().reconcile().get();

    EXPECT_EQ(
      rebuilt(), std::vector<model::transform_id>{model::transform_id(1)})
      << "revoking a capability left the running transform untouched";
}

TEST_F(ReconcilerTest, TheWatchFiresWithoutAnyoneCallingReconcile) {
    // The failure this rules out: everything below works, but nothing ever
    // calls it, so allowlist edits are silently inert. Nothing else in these
    // tests would notice, because they all drive reconcile() directly.
    set_allowlist({entry(sha_a)});
    add_transform(model::transform_id(1), sha_a);
    make_reconciler();
    reconciler().start();

    set_allowlist({});

    RPTEST_REQUIRE_EVENTUALLY(
      std::chrono::seconds(5), [this] { return !rebuilt().empty(); });
    EXPECT_EQ(rebuilt().front(), model::transform_id(1));
}

TEST_F(ReconcilerTest, RebuildsEveryAffectedTransformNotJustTheFirst) {
    // Two transforms share one binary, and a third is unrelated. Stopping
    // after the first would leave the second running with a capability the
    // allowlist no longer grants - and with one transform deployed, which is
    // the usual shape, the bug is invisible.
    set_allowlist({entry(sha_a), entry(sha_b)});
    add_transform(model::transform_id(1), sha_a);
    add_transform(model::transform_id(2), sha_a);
    add_transform(model::transform_id(3), sha_b);
    make_reconciler();

    set_allowlist({entry(sha_b)}); // sha_a revoked, sha_b untouched
    reconciler().reconcile().get();

    EXPECT_EQ(
      rebuilt(),
      (std::vector<model::transform_id>{
        model::transform_id(1), model::transform_id(2)}))
      << "expected both sha_a transforms and only those";
}

TEST_F(ReconcilerTest, AnUnrelatedEditRebuildsNothing) {
    // Rebuilding is not free - it drains and restarts a processor - so an edit
    // that does not touch a running binary must not disturb it.
    set_allowlist({entry(sha_a)});
    add_transform(model::transform_id(1), sha_a);
    make_reconciler();

    set_allowlist({entry(sha_a), entry(sha_b)}); // adds an unused entry
    reconciler().reconcile().get();

    EXPECT_TRUE(rebuilt().empty())
      << "an allowlist edit unrelated to this binary forced a rebuild";
}

TEST_F(ReconcilerTest, ATransformWithNoDigestIsNeverRebuilt) {
    set_allowlist({});
    add_transform_without_digest(model::transform_id(7));
    make_reconciler();

    set_allowlist({entry(sha_a)});
    reconciler().reconcile().get();

    EXPECT_TRUE(rebuilt().empty())
      << "a transform with no recorded digest could never have been trusted";
}

TEST_F(ReconcilerTest, ASecondReconcileDiffsAgainstWhatWasApplied) {
    // The snapshot has to advance on every reconcile. If it did not, the same
    // change would be re-detected forever and the transform would rebuild in
    // a loop - draining and restarting on every unrelated config update.
    set_allowlist({entry(sha_a)});
    add_transform(model::transform_id(1), sha_a);
    make_reconciler();

    set_allowlist({});
    reconciler().reconcile().get();
    ASSERT_EQ(rebuilt().size(), 1u);

    reconciler().reconcile().get();
    EXPECT_EQ(rebuilt().size(), 1u)
      << "the same revocation was applied twice; the snapshot did not advance";
}

TEST_F(ReconcilerTest, InvalidatesTheCompiledModuleBeforeRebuilding) {
    // Rebuilding alone does not revoke anything. The factory cache is
    // process-wide and holds weak references, so a compiled module stays
    // alive while any shard's processor holds it - and this reconciler is per
    // shard, rebuilding only its own. A shard could therefore drain, erase
    // and restart its processors and have the restart handed back the
    // still-live OLD-GRANT module, because a peer shard had not reconciled
    // yet. Silent: nothing fails, the transform simply keeps a capability the
    // allowlist no longer gives it.
    //
    // Ordering matters as much as the call. The restart is what asks for a
    // factory, so invalidation has to happen first or the restart hits a
    // cache that is still warm.
    set_allowlist({entry(sha_a)});
    add_transform(model::transform_id(1), sha_a);
    make_reconciler();

    set_allowlist({});
    reconciler().reconcile().get();

    EXPECT_EQ(invalidated(), std::vector<ss::sstring>{ss::sstring(sha_a)})
      << "the compiled module was not discarded, so a restart could reuse it";
    EXPECT_EQ(
      rebuilt(), std::vector<model::transform_id>{model::transform_id(1)});
}

TEST_F(ReconcilerTest, DoesNotInvalidateAnUnaffectedBinary) {
    // Invalidating costs a recompile on the next start, so an edge that does
    // not touch a running binary must not trigger one.
    set_allowlist({entry(sha_a)});
    add_transform(model::transform_id(1), sha_a);
    make_reconciler();

    set_allowlist({entry(sha_a), entry(sha_b)});
    reconciler().reconcile().get();

    EXPECT_TRUE(invalidated().empty())
      << "an unrelated allowlist edit forced a recompile";
}

} // namespace transform
