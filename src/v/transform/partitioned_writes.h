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

#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>

#include <algorithm>
#include <optional>
#include <utility>
#include <vector>

namespace transform {

/**
 * How many output partitions of one output topic a processor writes to at
 * once. Each write is a produce awaiting a quorum ack, so a flush cycle that
 * touches N partitions one after another costs N replication round trips: a
 * keyed transform fanning out to 80 partitions measured ~25ms per cycle that
 * way (2026-10-16), against ~0.8ms for the same records to one partition.
 */
inline constexpr size_t max_concurrent_partition_writes = 32;

namespace detail {
template<typename Item, typename Write>
ss::future<> write_in_order(std::vector<Item>& chain, Write& write) {
    for (auto& item : chain) {
        co_await write(item);
    }
}
} // namespace detail

/**
 * Write `items` with up to `max_concurrency` output partitions in flight at
 * once, and every partition's items strictly in the order given.
 *
 * Per-partition order is not optional: the sink stamps each batch with the
 * idempotent producer's next sequence number for its partition before the
 * produce is sent, so two writes to one partition in flight together could
 * arrive out of order and be rejected. Different partitions have independent
 * sequences and can overlap.
 *
 * `partition_of(item)` resolves an item's output partition; std::nullopt
 * (a sink that cannot say) puts the item in one chain with every other
 * unresolved item, written in order - which, for a sink that resolves
 * nothing, is exactly the old sequential behaviour. `write(item)` returns
 * the future of that item's write. If writes fail, the first failure is
 * returned once every chain has stopped.
 */
template<typename Item, typename PartitionOf, typename Write>
ss::future<> write_by_partition(
  std::vector<Item> items,
  PartitionOf partition_of,
  Write write,
  size_t max_concurrency) {
    std::vector<std::pair<std::optional<model::partition_id>, std::vector<Item>>>
      chains;
    for (auto& item : items) {
        auto p = partition_of(item);
        auto it = std::ranges::find_if(
          chains, [&p](const auto& chain) { return chain.first == p; });
        if (it == chains.end()) {
            chains.emplace_back(p, std::vector<Item>{});
            it = std::prev(chains.end());
        }
        it->second.push_back(std::move(item));
    }
    if (chains.size() == 1) {
        co_await detail::write_in_order(chains.front().second, write);
        co_return;
    }
    co_await ss::max_concurrent_for_each(
      chains, std::max<size_t>(max_concurrency, 1), [&write](auto& chain) {
          return detail::write_in_order(chain.second, write);
      });
}

} // namespace transform
