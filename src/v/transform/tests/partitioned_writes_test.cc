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

#include "transform/partitioned_writes.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <optional>
#include <stdexcept>
#include <vector>

namespace transform {
namespace {

using namespace std::chrono_literals;

struct item {
    std::optional<int> partition;
    int value;
};

// Records what write_by_partition did: per-partition order, and how many
// writes - overall and per partition - were in flight at once.
struct recorder {
    std::map<std::optional<int>, std::vector<int>> order;
    std::map<std::optional<int>, int> in_flight_on;
    int in_flight = 0;
    int max_in_flight = 0;
    int max_in_flight_one_partition = 0;

    ss::future<> write(item& it) {
        ++in_flight;
        max_in_flight = std::max(max_in_flight, in_flight);
        auto& on = in_flight_on[it.partition];
        ++on;
        max_in_flight_one_partition = std::max(max_in_flight_one_partition, on);
        // Long enough for every chain that can run to have started.
        co_await ss::sleep(2ms);
        order[it.partition].push_back(it.value);
        --in_flight_on[it.partition];
        --in_flight;
    }
};

ss::future<> run(std::vector<item> items, recorder& rec, size_t max_concurrency) {
    return write_by_partition(
      std::move(items),
      [](const item& it) -> std::optional<model::partition_id> {
          if (!it.partition) {
              return std::nullopt;
          }
          return model::partition_id(*it.partition);
      },
      [&rec](item& it) { return rec.write(it); },
      max_concurrency);
}

TEST(PartitionedWrites, EachPartitionInTheOrderGiven) {
    recorder rec;
    run({{0, 1}, {1, 2}, {0, 3}, {1, 4}, {0, 5}, {2, 6}}, rec, 32).get();
    EXPECT_EQ(rec.order[0], (std::vector<int>{1, 3, 5}));
    EXPECT_EQ(rec.order[1], (std::vector<int>{2, 4}));
    EXPECT_EQ(rec.order[2], (std::vector<int>{6}));
}

TEST(PartitionedWrites, DifferentPartitionsAreWrittenConcurrently) {
    recorder rec;
    std::vector<item> items;
    for (int p = 0; p < 8; ++p) {
        items.push_back({p, p});
    }
    run(std::move(items), rec, 32).get();
    EXPECT_EQ(rec.max_in_flight, 8);
}

// The idempotent producer's sequence numbers are per partition and stamped
// before each produce is sent, so one partition must never have two writes
// in flight.
TEST(PartitionedWrites, NeverTwoWritesToOnePartitionAtOnce) {
    recorder rec;
    std::vector<item> items;
    for (int i = 0; i < 40; ++i) {
        items.push_back({i % 4, i});
    }
    run(std::move(items), rec, 32).get();
    EXPECT_EQ(rec.max_in_flight_one_partition, 1);
    EXPECT_EQ(rec.max_in_flight, 4);
    for (int p = 0; p < 4; ++p) {
        std::vector<int> want;
        for (int i = p; i < 40; i += 4) {
            want.push_back(i);
        }
        EXPECT_EQ(rec.order[p], want);
    }
}

TEST(PartitionedWrites, ConcurrencyIsBounded) {
    recorder rec;
    std::vector<item> items;
    for (int p = 0; p < 10; ++p) {
        items.push_back({p, p});
    }
    run(std::move(items), rec, 3).get();
    EXPECT_EQ(rec.max_in_flight, 3);
    EXPECT_EQ(rec.order.size(), 10);
}

// A sink that cannot resolve partitions keeps the old behaviour: every write
// one after another, in the order given.
TEST(PartitionedWrites, UnresolvedPartitionsAreWrittenInOrderOneAtATime) {
    recorder rec;
    run({{std::nullopt, 1}, {std::nullopt, 2}, {std::nullopt, 3}}, rec, 32)
      .get();
    EXPECT_EQ(rec.max_in_flight, 1);
    EXPECT_EQ(rec.order[std::nullopt], (std::vector<int>{1, 2, 3}));
}

TEST(PartitionedWrites, AFailedWriteFailsTheCall) {
    std::vector<item> items{{0, 1}, {1, 2}, {1, 3}};
    auto f = write_by_partition(
      std::move(items),
      [](const item& it) { return model::partition_id(*it.partition); },
      [](item& it) -> ss::future<> {
          if (it.value == 2) {
              return ss::make_exception_future<>(std::runtime_error("boom"));
          }
          return ss::now();
      },
      32);
    EXPECT_THROW(f.get(), std::runtime_error);
}

} // namespace
} // namespace transform
