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
 * There is one instance per shard. A push is shard-local: the producing
 * transform processor calls push() on the shard it runs on, and every
 * subscription for that (topic, partition) must therefore live on the same
 * shard - consumers connect/subscribe on the shard that owns the partition's
 * data. This keeps the hot path free of any cross-shard coordination.
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

    // Fan data out to every consumer subscribed to ntp on this shard.
    // Synchronous and never blocks - each subscription applies its own bound
    // and drops rather than applying backpressure to the producer. Safe to
    // call from the transform hot path. Returns immediately, without touching
    // data, when nothing subscribes to ntp, so producers pay nothing for the
    // common no-consumer case.
    void push(const model::ntp& ntp, const iobuf& data);

    // Register a consumer for ntp, returning its subscription id. Must be
    // called on the shard that owns the data for ntp.
    //
    // Non-owning: the caller owns the subscription and must call
    // remove_subscription before destroying it. This is what lets owning
    // consumers (a wasm host module, or the TCP server's per-connection
    // state) tie a subscription's lifetime to their own - including running
    // a background drain fiber - without the relay ever outliving or freeing
    // it under them. Safety comes from shard-locality: push() and
    // remove_subscription() both run synchronously on this one shard, so a
    // deliver() can never be mid-flight against a subscription being
    // removed.
    uint32_t add_subscription(model::ntp ntp, subscription*);

    // Remove a subscription previously registered with add_subscription().
    // Does not destroy it - that's the owner's job, after this returns. A
    // no-op if the id is unknown (e.g. already removed).
    void remove_subscription(uint32_t id);

    // Number of live subscriptions for ntp on this shard - exposed for
    // tests and metrics.
    size_t subscription_count(const model::ntp& ntp) const;

    probe& get_probe() { return _probe; }

private:
    absl::flat_hash_map<model::ntp, absl::flat_hash_map<uint32_t, subscription*>>
      _by_ntp;
    // Reverse index so remove_subscription() doesn't scan every ntp.
    absl::flat_hash_map<uint32_t, model::ntp> _ntp_by_id;
    uint32_t _next_id{0};
    probe _probe;
    config _cfg;
    // Engaged when _cfg.tcp_enabled - the per-shard listener external
    // consumers connect to.
    std::optional<tcp_server> _tcp;
};

} // namespace relay
