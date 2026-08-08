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

#include "model/fundamental.h"
#include "model/namespace.h"
#include "relay/relay_service.h"
#include "relay/subscription.h"
#include "test_utils/async.h"

#include <gtest/gtest.h>

namespace relay {
namespace {

class test_subscription final : public subscription {
public:
    bool deliver(const iobuf& data) final {
        ++calls;
        last_size = data.size_bytes();
        return deliver_result;
    }

    int calls = 0;
    size_t last_size = 0;
    bool deliver_result = true;
};

model::ntp test_ntp(int p) {
    return {
      model::kafka_namespace,
      model::topic{"test_topic"},
      model::partition_id{p}};
}

iobuf make_data(size_t n) {
    iobuf b;
    auto s = ss::sstring(n, 'x');
    b.append(s.data(), n);
    return b;
}

struct relay_fixture : public ::testing::Test {
    // No TCP listener in tests - exercises the core fan-out only.
    relay::service svc{relay::service::config{.tcp_enabled = false}};

    void SetUp() override { svc.start().get(); }
    void TearDown() override { svc.stop().get(); }
};

TEST_F(relay_fixture, PushWithNoSubscribersIsANoop) {
    // Must not crash, touch the data, or count a delivery.
    svc.push(test_ntp(0), make_data(64));
    EXPECT_EQ(svc.subscription_count(test_ntp(0)), 0);
    EXPECT_EQ(svc.get_probe().delivered(), 0);
}

TEST_F(relay_fixture, DeliversToAllSubscribersOnTheNtp) {
    test_subscription a;
    test_subscription b;
    svc.add_subscription(test_ntp(0), &a);
    svc.add_subscription(test_ntp(0), &b);
    // A different partition must not receive it.
    test_subscription other;
    svc.add_subscription(test_ntp(1), &other);

    svc.push(test_ntp(0), make_data(32));

    EXPECT_EQ(a.calls, 1);
    EXPECT_EQ(b.calls, 1);
    EXPECT_EQ(other.calls, 0);
    EXPECT_EQ(a.last_size, 32);
}

TEST_F(relay_fixture, RemoveSubscriptionStopsDelivery) {
    test_subscription a;
    auto id = svc.add_subscription(test_ntp(0), &a);
    svc.push(test_ntp(0), make_data(8));
    EXPECT_EQ(a.calls, 1);

    svc.remove_subscription(id);
    EXPECT_EQ(svc.subscription_count(test_ntp(0)), 0);
    svc.push(test_ntp(0), make_data(8));
    // No further deliveries after removal.
    EXPECT_EQ(a.calls, 1);
}

TEST_F(relay_fixture, RemoveUnknownIdIsANoop) {
    svc.remove_subscription(9999);
}

TEST_F(relay_fixture, DroppedDeliveryIsCountedNotFatal) {
    test_subscription a;
    a.deliver_result = false;
    svc.add_subscription(test_ntp(0), &a);
    // push() must not throw or block when a consumer is backlogged.
    svc.push(test_ntp(0), make_data(16));
    EXPECT_EQ(a.calls, 1);
    EXPECT_EQ(svc.get_probe().dropped(), 1);
}

} // namespace
} // namespace relay
