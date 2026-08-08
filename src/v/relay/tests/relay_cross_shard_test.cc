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

#include "base/vassert.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "relay/relay_service.h"
#include "relay/subscription.h"
#include "test_utils/async.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <utility>

// Exercises genuine cross-shard delivery, unlike relay_service_test.cc
// (which runs single-shard, since relay::service is constructed there
// directly rather than through a real sharded<>). Needs its own binary with
// cpu > 1 - see BUILD.
namespace relay {
namespace {

class test_subscription final : public subscription {
public:
    bool deliver(const iobuf& data) final {
        ++calls;
        last_size = data.size_bytes();
        return true;
    }

    int calls = 0;
    size_t last_size = 0;
};

model::ntp test_ntp() {
    return {
      model::kafka_namespace,
      model::topic{"cross_shard_test_topic"},
      model::partition_id{0}};
}

iobuf make_data(size_t n) {
    iobuf b;
    auto s = ss::sstring(n, 'x');
    b.append(s.data(), n);
    return b;
}

// thread_local so each shard only ever touches its own copy - matching
// relay::service's own no-lock, per-shard-write invariant for the state
// backing these tests.
thread_local test_subscription g_sub;
thread_local std::optional<uint32_t> g_sub_id;

} // namespace

class relay_cross_shard_fixture : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        vassert(
          ss::this_smp_shard_count() > 1,
          "this test expects multiple shards");
    }

    void SetUp() override {
        svcs.start(service::config{.tcp_enabled = false}).get();
    }

    void TearDown() override { svcs.stop().get(); }

    ss::sharded<service> svcs;
};

TEST_F(relay_cross_shard_fixture, PushFromOneShardReachesSubscriberOnAnother) {
    auto ntp = test_ntp();
    svcs
      .invoke_on(
        1, [ntp](service& s) { g_sub_id = s.add_subscription(ntp, &g_sub); })
      .get();

    // add_subscription()'s cross-shard broadcast is fire-and-forget, so it
    // may not have reached this shard's _shards_with_subscribers yet by the
    // time the invoke_on above returns. Retry the push itself inside the
    // spin-wait rather than assuming a single push suffices.
    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(5),
      [this, ntp] {
          svcs.local().push(ntp, make_data(48));
          return svcs.invoke_on(
            1, [](service&) { return g_sub.calls > 0; });
      })
      .get();

    auto [calls, last_size] = svcs
                                 .invoke_on(1, [](service&) {
                                     return std::make_pair(
                                       g_sub.calls, g_sub.last_size);
                                 })
                                 .get();
    EXPECT_GE(calls, 1);
    EXPECT_EQ(last_size, 48);

    svcs
      .invoke_on(
        1, [](service& s) { s.remove_subscription(*g_sub_id); })
      .get();
}

// Added 2026-08-29 alongside the push() rewrite that stopped copying the
// payload once per remote shard. push() now makes ONE copy, keeps it owned by
// the producer's shard, and hands every remote shard a plain read-only
// reference to it - so this is the test that matters for that change: several
// shards reading the same producer-owned buffer concurrently, all of them
// having to see the correct bytes, and the buffer having to outlive the last
// read. A lifetime bug here shows up as a wrong size, wrong content, or a
// crash under ASAN rather than as a wrong count.
TEST_F(relay_cross_shard_fixture, PushReachesSubscribersOnEveryRemoteShard) {
    auto ntp = test_ntp();
    const auto shards = ss::this_smp_shard_count();
    ASSERT_GT(shards, 1u);

    // Subscribe on every shard except the producer's (shard 0).
    for (unsigned sh = 1; sh < shards; ++sh) {
        svcs
          .invoke_on(
            sh, [ntp](service& s) { g_sub_id = s.add_subscription(ntp, &g_sub); })
          .get();
    }

    // A distinctive size, so a mixed-up or freed buffer is visible rather than
    // coincidentally right.
    constexpr size_t payload = 1237;
    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(10),
      [this, ntp, shards] {
          svcs.local().push(ntp, make_data(payload));
          return svcs.invoke_on(shards - 1, [](service&) {
              return g_sub.calls > 0;
          });
      })
      .get();

    // Push a few more times so every remote shard has certainly been reached,
    // then assert each one saw the right bytes. The spin-wait above only
    // guarantees the LAST shard got one.
    for (int i = 0; i < 5; ++i) {
        svcs.local().push(ntp, make_data(payload));
    }
    // Let the fire-and-forget cross-shard dispatches drain.
    ss::sleep(std::chrono::milliseconds(200)).get();

    for (unsigned sh = 1; sh < shards; ++sh) {
        auto [calls, last_size]
          = svcs
              .invoke_on(
                sh,
                [](service&) {
                    return std::make_pair(g_sub.calls, g_sub.last_size);
                })
              .get();
        EXPECT_GE(calls, 1) << "shard " << sh << " received nothing";
        EXPECT_EQ(last_size, payload)
          << "shard " << sh << " saw the wrong payload size, which is what a "
             "prematurely freed or mis-shared buffer looks like";
    }

    for (unsigned sh = 1; sh < shards; ++sh) {
        svcs.invoke_on(sh, [](service& s) { s.remove_subscription(*g_sub_id); })
          .get();
    }
}

TEST_F(
  relay_cross_shard_fixture, RemovedSubscriberStopsReceivingAcrossShards) {
    auto ntp = test_ntp();
    svcs
      .invoke_on(
        1, [ntp](service& s) { g_sub_id = s.add_subscription(ntp, &g_sub); })
      .get();

    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(5),
      [this, ntp] {
          svcs.local().push(ntp, make_data(16));
          return svcs.invoke_on(
            1, [](service&) { return g_sub.calls > 0; });
      })
      .get();

    svcs
      .invoke_on(
        1, [](service& s) { s.remove_subscription(*g_sub_id); })
      .get();

    // remove_subscription()'s broadcast is also fire-and-forget, and there's
    // no public way to observe when it's landed on every shard - a short
    // sleep is simplest here, generous relative to an in-process smp queue
    // hop, before treating "no further delivery" as meaningful rather than
    // racing a push against an in-flight unsubscribe.
    ss::sleep(std::chrono::milliseconds(200)).get();

    auto calls_before = svcs
                           .invoke_on(
                             1, [](service&) { return g_sub.calls; })
                           .get();
    svcs.local().push(ntp, make_data(16));
    // Give any errant delivery a chance to land before checking it didn't.
    ss::sleep(std::chrono::milliseconds(50)).get();
    auto calls_after = svcs
                          .invoke_on(1, [](service&) { return g_sub.calls; })
                          .get();
    EXPECT_EQ(calls_after, calls_before);
}

} // namespace relay
