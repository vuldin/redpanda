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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "relay/probe.h"
#include "relay/subscription.h"
#include "relay/tcp_server.h"

#include <seastar/core/sharded.hh>

#include <cstdint>
#include <memory>
#include <optional>

namespace relay {

/**
 * The relay service fans pushed data out to subscribed consumers, bypassing
 * the Kafka fetch path for consumers that need microsecond latency and/or
 * high fanout. Producers (today, transform processors) push bytes; consumers
 * receive them either via a wasm guest's shared-memory region (in-process,
 * zero-copy) or over TCP (external).
 *
 * There is one instance per shard. A subscription always lives on the shard
 * that owns the data for its ntp - consumers connect/subscribe on that
 * shard, never elsewhere. push() itself is still local-first: the producing
 * transform processor calls push() on the shard it runs on, and any
 * subscriber on that same shard is delivered to synchronously, in-process,
 * with no cross-shard hop at all. Subscribers on *other* shards - the
 * mechanism that lets consumers spread across cores instead of piling onto
 * one - are delivered to via a fire-and-forget cross-shard dispatch (see
 * push()'s implementation): the producing shard never blocks on it, and a
 * subscriber that unsubscribes between dispatch and delivery is simply
 * absent from the target shard's own table by the time delivery runs there -
 * the same safety property same-shard delivery already had.
 *
 * The durable Kafka write still happens independently, so ordinary Kafka
 * consumers fetching the same topics are unaffected: the relay is an
 * additional, optimistic fast path, not a replacement for the log.
 */
class service : public ss::peering_sharded_service<service> {
public:
    struct config {
        // Whether to run the TCP listener for external consumers. Wasm
        // consumers (shared-memory subscriptions) work regardless.
        bool tcp_enabled = true;
        // Base port for the TCP listener; each shard listens on
        // tcp_port + ss::this_shard_id(), so a consumer connects to the
        // shard that owns its partition's data.
        uint16_t tcp_port = 9093;
        // Per-consumer bound on queued-but-unsent records before the oldest
        // start being dropped.
        size_t max_queue_size = 1024;
    };

    service();
    explicit service(config cfg);
    service(const service&) = delete;
    service& operator=(const service&) = delete;
    service(service&&) = delete;
    service& operator=(service&&) = delete;
    ~service();

    ss::future<> start();
    ss::future<> stop();

    // Fan data out to every consumer subscribed to ntp, on this shard and any
    // other. Synchronous and never blocks for the caller - same-shard
    // delivery happens in-process before this returns; delivery to
    // subscribers on other shards is dispatched in the background and does
    // not delay the caller. Each subscription applies its own bound and
    // drops rather than applying backpressure to the producer. Safe to call
    // from the transform hot path. When nothing subscribes to ntp anywhere,
    // this costs two cheap hash lookups (this shard's own subscriber table,
    // and the table of which other shards have subscribers) rather than the
    // single lookup it used to be - still negligible, but no longer
    // literally free.
    void push(const model::ntp& ntp, const iobuf& data);

    // Register a consumer for ntp, returning its subscription id. Must be
    // called on the shard that owns the data for ntp - a subscription
    // itself never moves shards; only push()'s delivery of data to it can
    // cross shards (see push()).
    //
    // Non-owning: the caller owns the subscription and must call
    // remove_subscription before destroying it. This is what lets owning
    // consumers (a wasm host module, or the TCP server's per-connection
    // state) tie a subscription's lifetime to their own - including running
    // a background drain fiber - without the relay ever outliving or freeing
    // it under them. Safety comes from shard-locality: a subscription
    // pointer is only ever touched on the shard it was registered on, by
    // push()'s same-shard path or by remove_subscription() itself, both of
    // which run synchronously on that one shard, so a deliver() can never be
    // mid-flight against a subscription being removed. Cross-shard delivery
    // never touches the pointer at all - it re-looks-up the target shard's
    // own table by ntp at delivery time instead, so a subscriber removed
    // after a cross-shard push was dispatched but before it's delivered is
    // simply no longer in that table by then.
    uint32_t add_subscription(model::ntp ntp, subscription*);

    // Remove a subscription previously registered with add_subscription().
    // Does not destroy it - that's the owner's job, after this returns. A
    // no-op if the id is unknown (e.g. already removed).
    void remove_subscription(uint32_t id);

    // Number of live subscriptions for ntp on this shard - exposed for
    // tests and metrics.
    size_t subscription_count(const model::ntp& ntp) const;

    probe& get_probe() { return _probe; }

    // The same per-consumer bound tcp_subscription applies to its own queue
    // - exposed so other subscription implementations (relay_source) can
    // apply the identical bound instead of growing without limit.
    size_t max_queue_size() const { return _cfg.max_queue_size; }

private:
    // Deliver to this shard's own subscribers for ntp only - the shared body
    // between push()'s local path and the closure it dispatches to other
    // shards.
    //
    // `dispatched_at` is the producer shard's clock reading taken just before
    // it began submitting, and is used only to record crossshard_transit. It
    // is defaulted/left unset on push()'s LOCAL path, where there is no
    // transit to measure, and on any caller that has stage metrics off. A
    // time_point is a trivially copyable scalar, so passing it by value into a
    // cross-shard lambda is safe (unlike anything refcounted).
    void deliver_locally(
      const model::ntp& ntp,
      const iobuf& data,
      ss::steady_clock_type::time_point dispatched_at
      = ss::steady_clock_type::time_point{},
      // The producing transform's emit instant, used for the emit_to_guest
      // span. Distinct from dispatched_at, which is taken later (after the
      // payload copy) and only measures the cross-shard hop.
      ss::steady_clock_type::time_point pushed_at
      = ss::steady_clock_type::time_point{});

    absl::
      flat_hash_map<model::ntp, absl::flat_hash_map<uint32_t, subscription*>>
        _by_ntp;
    // Reverse index so remove_subscription() doesn't scan every ntp.
    absl::flat_hash_map<uint32_t, model::ntp> _ntp_by_id;
    // Which other shards (this one excluded) have at least one subscriber
    // for a given ntp, replicated to every shard via add_subscription()/
    // remove_subscription() broadcasting local subscriber-count transitions.
    // Each shard only ever writes its own shard id into every copy of this
    // map, never another shard's - so no lock is needed, matching the same
    // pattern used by partition_leaders_table/node_status_table elsewhere in
    // this codebase. Read on every push() to decide whether cross-shard
    // dispatch is needed at all.
    absl::flat_hash_map<model::ntp, absl::flat_hash_set<ss::shard_id>>
      _shards_with_subscribers;
    uint32_t _next_id{0};
    probe _probe;
    config _cfg;
    // Engaged when _cfg.tcp_enabled - the per-shard listener external
    // consumers connect to.
    std::optional<tcp_server> _tcp;
};

} // namespace relay
