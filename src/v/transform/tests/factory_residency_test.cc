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

#include "config/mock_property.h"
#include "model/metadata.h"
#include "model/transform.h"
#include "test_utils/async.h"
#include "test_utils/test.h"
#include "transform/factory_residency.h"

#include <seastar/core/manual_clock.hh>

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_set.h>
#include <gtest/gtest.h>

#include <chrono>
#include <vector>

// What these cover is the derivation and the diff: which modules belong on
// this broker, and what has to happen to the ones already held when that
// answer changes. The wiring that supplies the real answers lives in
// transform::service, which no test target links - so it is deliberately kept
// to an adapter there and an interface here.

namespace transform {
namespace {

using namespace std::chrono_literals;

model::topic_namespace topic(std::string_view name) {
    return {model::kafka_namespace, model::topic(ss::sstring(name))};
}

class fake_store final : public factory_store {
public:
    ss::future<bool> pin(model::transform_metadata meta) final {
        ++pin_attempts;
        attempted.push_back(meta.source_ptr);
        if (refuse) {
            return ss::make_ready_future<bool>(false);
        }
        _held.insert(meta.source_ptr);
        return ss::make_ready_future<bool>(true);
    }
    void unpin(model::offset offset) final {
        unpinned.push_back(offset);
        _held.erase(offset);
    }
    std::vector<model::offset> resident() const final {
        return {_held.begin(), _held.end()};
    }
    bool busy() const final { return is_busy; }
    void set_max_resident(size_t max) final { max_resident = max; }

    bool holds(model::offset offset) const { return _held.contains(offset); }
    size_t held_count() const { return _held.size(); }

    int pin_attempts = 0;
    bool refuse = false;
    bool is_busy = false;
    std::optional<size_t> max_resident;
    std::vector<model::offset> attempted;
    std::vector<model::offset> unpinned;

private:
    absl::btree_set<model::offset> _held;
};

class ResidencyTest : public ::testing::Test {
public:
    void TearDown() override {
        if (_r) {
            _r->stop().get();
            _r.reset();
        }
    }

    void make_residency(size_t max = 8) {
        _max = std::make_unique<config::mock_property<size_t>>(max);
        _r = std::make_unique<factory_residency<ss::manual_clock>>(
          &_store,
          _max->bind(),
          [this] { return _transforms; },
          [this](const model::topic_namespace& tp_ns) {
              return _replicated.contains(tp_ns);
          });
    }

    // The behavioural tests drive this directly. reconcile() is public for
    // exactly this reason: going through the timers would make every
    // assertion below a statement about scheduling as well as about the diff.
    void reconcile() { _r->reconcile().get(); }

    // Advances the fake clock in steps until a sweep actually completes,
    // rather than sleeping and hoping.
    //
    // Stepping rather than jumping, because the resweep timer only schedules
    // a sweep: the settle that follows is armed from inside that timer's own
    // callback, so it does not exist yet at the moment the timer fires and
    // cannot be waited out in a single advance.
    void advance_until_sweep(ss::manual_clock::duration step) {
        auto before = _r->reconcile_count();
        tests::cooperative_spin_wait_with_timeout(5s, [this, before, step] {
            if (_r->reconcile_count() > before) {
                return true;
            }
            ss::manual_clock::advance(step);
            return false;
        }).get();
    }

    model::offset deploy(
      model::transform_id id,
      std::string_view input,
      model::offset source,
      model::is_transform_paused paused = model::is_transform_paused::no) {
        model::transform_metadata m;
        m.name = model::transform_name{
          ss::sstring("xform-") + std::to_string(id())};
        m.input_topic = topic(input);
        m.source_ptr = source;
        m.paused = paused;
        _transforms.insert_or_assign(id, std::move(m));
        return source;
    }

    void make_relay_pinned(model::transform_id id, ss::shard_id shard) {
        auto it = _transforms.find(id);
        it->second.environment.emplace("RELAY_SOURCE", "1");
        it->second.environment.emplace(
          "RELAY_TARGET_SHARD", std::to_string(shard));
    }

    void replicate(std::string_view t) { _replicated.insert(topic(t)); }
    void stop_replicating(std::string_view t) { _replicated.erase(topic(t)); }
    void remove_transform(model::transform_id id) { _transforms.erase(id); }

    fake_store _store;
    factory_residency<ss::manual_clock>::transform_map _transforms;
    absl::flat_hash_set<model::topic_namespace> _replicated;
    std::unique_ptr<config::mock_property<size_t>> _max;
    std::unique_ptr<factory_residency<ss::manual_clock>> _r;
};

} // namespace

TEST_F(ResidencyTest, KeepsAModuleWhoseInputThisBrokerReplicates) {
    auto source = deploy(model::transform_id(1), "in", model::offset(10));
    replicate("in");
    make_residency();

    reconcile();

    EXPECT_TRUE(_store.holds(source));
}

TEST_F(ResidencyTest, IgnoresATransformThisBrokerDoesNotReplicate) {
    // The entire justification for holding a module is that a partition could
    // move here. Without a local replica it cannot.
    deploy(model::transform_id(1), "elsewhere", model::offset(10));
    make_residency();

    reconcile();

    EXPECT_EQ(_store.held_count(), 0u);
    EXPECT_EQ(_store.pin_attempts, 0) << "compiled a module for no reason";
}

TEST_F(ResidencyTest, IgnoresAPausedTransform) {
    // Nothing is going to start it, so there is no cold start to pre-empt.
    deploy(
      model::transform_id(1),
      "in",
      model::offset(10),
      model::is_transform_paused::yes);
    replicate("in");
    make_residency();

    reconcile();

    EXPECT_EQ(_store.pin_attempts, 0);
}

TEST_F(ResidencyTest, IgnoresATransformPinnedToAShardByTheRelay) {
    // A shard-pinned relay-sourced transform is kept alive on that shard
    // regardless of leadership, so its own processor already holds its module
    // for as long as the transform exists. A second reference would spend a
    // slot on the one module that cannot go cold.
    deploy(model::transform_id(1), "in", model::offset(10));
    make_relay_pinned(model::transform_id(1), 2);
    replicate("in");
    make_residency();

    reconcile();

    EXPECT_EQ(_store.pin_attempts, 0);
}

TEST_F(ResidencyTest, ARedeployMovesTheHeldModuleToTheNewOffset) {
    // Falls out of the diff rather than needing its own handler: a redeploy
    // moves source_ptr, so the old offset is no longer wanted and the new one
    // is not yet held.
    auto old_source = deploy(model::transform_id(1), "in", model::offset(10));
    replicate("in");
    make_residency();
    reconcile();
    ASSERT_TRUE(_store.holds(old_source));

    auto new_source = deploy(model::transform_id(1), "in", model::offset(20));
    reconcile();

    EXPECT_FALSE(_store.holds(old_source)) << "kept the superseded module";
    EXPECT_TRUE(_store.holds(new_source));
}

TEST_F(ResidencyTest, GivesBackAModuleWhoseTransformWasDeleted) {
    auto source = deploy(model::transform_id(1), "in", model::offset(10));
    replicate("in");
    make_residency();
    reconcile();
    ASSERT_TRUE(_store.holds(source));

    remove_transform(model::transform_id(1));
    reconcile();

    EXPECT_FALSE(_store.holds(source));
}

TEST_F(ResidencyTest, GivesBackAModuleOnceTheLastLocalReplicaIsGone) {
    auto source = deploy(model::transform_id(1), "in", model::offset(10));
    replicate("in");
    make_residency();
    reconcile();
    ASSERT_TRUE(_store.holds(source));

    stop_replicating("in");
    reconcile();

    EXPECT_FALSE(_store.holds(source));
}

TEST_F(ResidencyTest, KeepsAModuleWantedByAnyOneOfSeveralTransforms) {
    // Two transforms, one input topic each, sharing a deploy offset is not
    // the case here - this is the case where one of two topics stops being
    // replicated and the other still is, so the module stays.
    auto a = deploy(model::transform_id(1), "in-a", model::offset(10));
    auto b = deploy(model::transform_id(2), "in-b", model::offset(20));
    replicate("in-a");
    replicate("in-b");
    make_residency();
    reconcile();
    ASSERT_EQ(_store.held_count(), 2u);

    stop_replicating("in-a");
    reconcile();

    EXPECT_FALSE(_store.holds(a));
    EXPECT_TRUE(_store.holds(b)) << "dropped an unrelated module";
}

TEST_F(ResidencyTest, TurningTheBoundToZeroGivesEverythingBack) {
    // An operator turning residency off has to get the memory back, not wait
    // for each module to be invalidated by something else.
    auto source = deploy(model::transform_id(1), "in", model::offset(10));
    replicate("in");
    make_residency(4);
    reconcile();
    ASSERT_TRUE(_store.holds(source));

    _max->update(0);
    reconcile();

    EXPECT_EQ(_store.held_count(), 0u);
}

TEST_F(ResidencyTest, DisabledByDefaultDoesNotEvenCompile) {
    // The default is off, and off has to cost nothing at all - not a fetch,
    // not a refused pin that climbs a failure counter every sweep.
    deploy(model::transform_id(1), "in", model::offset(10));
    replicate("in");
    make_residency(0);

    reconcile();

    EXPECT_EQ(_store.pin_attempts, 0);
    EXPECT_EQ(_store.held_count(), 0u);
}

TEST_F(ResidencyTest, StandsAsideWhileATransformIsWaitingToCompile) {
    // Every compile in the process runs on one alien thread. Queueing a
    // speculative one in front of a transform that is actually starting would
    // make a real cold start slower in order to pre-empt one that may never
    // happen.
    deploy(model::transform_id(1), "in", model::offset(10));
    replicate("in");
    make_residency();
    _store.is_busy = true;

    reconcile();

    EXPECT_EQ(_store.pin_attempts, 0);

    // And picks it up once the compile thread is free again.
    _store.is_busy = false;
    reconcile();
    EXPECT_EQ(_store.pin_attempts, 1);
}

TEST_F(ResidencyTest, ARefusedModuleIsTriedAgainOnALaterSweep) {
    // A refusal is not an error and not sticky: the bound may have room by
    // the next sweep, because residency is not the only thing that gives
    // modules up.
    deploy(model::transform_id(1), "in", model::offset(10));
    replicate("in");
    make_residency();
    _store.refuse = true;
    reconcile();
    ASSERT_EQ(_store.pin_attempts, 1);
    ASSERT_EQ(_store.held_count(), 0u);

    _store.refuse = false;
    reconcile();

    EXPECT_EQ(_store.pin_attempts, 2) << "gave up on a refused module";
    EXPECT_EQ(_store.held_count(), 1u);
}

TEST_F(ResidencyTest, StartPushesTheBoundToTheStore) {
    // The store enforces the bound, so it has to learn the configured value
    // without waiting for a sweep to happen to run.
    make_residency(3);
    ASSERT_FALSE(_store.max_resident.has_value());

    _r->start();

    EXPECT_EQ(_store.max_resident, 3u);
}

TEST_F(ResidencyTest, AChangedBoundReachesTheStoreImmediately) {
    // Lowering it has to release modules now rather than whenever a sweep
    // gets around to them, which is why the bound is pushed before the sweep
    // is scheduled rather than during it.
    make_residency(3);
    _r->start();
    ASSERT_EQ(_store.max_resident, 3u);

    _max->update(1);

    EXPECT_EQ(_store.max_resident, 1u);
}

TEST_F(ResidencyTest, ABurstOfPokesCollapsesIntoOneSweep) {
    // The boot-time replay notifies for every existing partition, and a
    // leadership move hands several over at once. Sweeping per notification
    // would derive the same answer repeatedly.
    deploy(model::transform_id(1), "in", model::offset(10));
    replicate("in");
    make_residency();
    _r->start();

    for (int i = 0; i < 20; ++i) {
        _r->poke();
    }
    advance_until_sweep(11s);

    EXPECT_EQ(_r->reconcile_count(), 1u) << "swept more than once for a burst";
    EXPECT_EQ(_store.pin_attempts, 1);
}

TEST_F(ResidencyTest, SweepsOnItsOwnWithNoNotificationAtAll) {
    // The backstop for what no notification covers: a replica this broker
    // acquired while it was not running, or a notification dropped.
    deploy(model::transform_id(1), "in", model::offset(10));
    replicate("in");
    make_residency();
    _r->start();
    // Let start()'s own sweep happen first, so what follows is attributable
    // to the resweep alone.
    advance_until_sweep(11s);
    ASSERT_EQ(_r->reconcile_count(), 1u);

    deploy(model::transform_id(2), "in-2", model::offset(20));
    replicate("in-2");
    // Nothing pokes. The only thing that can produce another sweep is the
    // resweep timer, five minutes out, and then its settle.
    advance_until_sweep(11s);

    EXPECT_TRUE(_store.holds(model::offset(20)))
      << "nothing picked up a change no notification covered";
}

} // namespace transform
