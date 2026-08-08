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

#include <seastar/core/lowres_clock.hh>

#include "base/seastarx.h"
#include "bytes/iobuf.h"

namespace relay {

/**
 * A single consumer of relayed data for one (topic, partition).
 *
 * deliver() is always called on the shard that owns this subscription, from
 * the hot path of whatever is producing the data (today, a transform
 * processor). It must never block: implementations either hand the bytes off
 * synchronously (a wasm guest's shared-memory region) or enqueue them for a
 * background drain (an external TCP consumer), applying their own bound and
 * dropping rather than growing without limit when the consumer falls behind.
 */
class subscription {
public:
    subscription() = default;
    subscription(const subscription&) = delete;
    subscription& operator=(const subscription&) = delete;
    subscription(subscription&&) = delete;
    subscription& operator=(subscription&&) = delete;
    virtual ~subscription() = default;

    // Returns false if the data was dropped because this consumer is
    // backlogged - callers use this only for metrics, never for flow
    // control: a producer must never slow down for a slow relay consumer.
    virtual bool deliver(const iobuf& data) = 0;

    /// Timestamped variant, used to measure the ONE span that matters for the
    /// in-broker consumer: the matcher's emit (push() entry) through to the
    /// consuming guest actually dequeuing the record.
    ///
    /// It exists because that span was previously only ever ESTIMATED, by
    /// summing separately-measured stage averages - which both double-counted
    /// (dispatch overlaps transit) and missed any time falling between stages.
    /// One histogram on one clock replaces three inferred terms.
    ///
    /// Default forwards to the untimed form, so only subscriptions that want
    /// the push time need to override it - tcp_subscription, the wasm
    /// relay_consumer_module and the test doubles are unaffected.
    virtual bool
    deliver(const iobuf& data, ss::steady_clock_type::time_point pushed_at) {
        (void)pushed_at;
        return deliver(data);
    }
};

} // namespace relay
