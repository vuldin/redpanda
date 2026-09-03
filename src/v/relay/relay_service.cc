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

#include "relay/relay_service.h"

#include "base/vlog.h"
#include "config/configuration.h"
#include "relay/logger.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <chrono>
#include <vector>

namespace relay {

service::service() = default;
service::service(config cfg)
  : _cfg(cfg) {}
service::~service() = default;

ss::future<> service::start() {
    _probe.setup_metrics();
    if (_cfg.tcp_enabled) {
        // Each shard listens on base port + shard id, so a consumer connects
        // to the shard that owns its partition's data (shard-local, no
        // cross-shard hops in the push path).
        uint16_t port = _cfg.tcp_port + ss::this_shard_id();
        _tcp.emplace(this, port, _cfg.max_queue_size);
        co_await _tcp->start();
    }
    co_return;
}

ss::future<> service::stop() {
    if (_tcp) {
        co_await _tcp->stop();
        _tcp.reset();
    }
    _by_ntp.clear();
    _ntp_by_id.clear();
    co_return;
}

namespace {
// invoke_on/invoke_on_all targets can arrive after the target shard's own
// service::stop() has already torn down its sharded<> instance - a normal
// race during shutdown, not a bug. Swallow it; there's nothing to deliver to
// or update on a shard that's already gone.
void ignore_no_sharded_instance(const std::exception_ptr& ex) {
    try {
        std::rethrow_exception(ex);
    } catch (const ss::no_sharded_instance_exception&) {
    }
}
} // namespace

void service::deliver_locally(
  const model::ntp& ntp,
  const iobuf& data,
  ss::steady_clock_type::time_point dispatched_at,
  ss::steady_clock_type::time_point pushed_at) {
    auto it = _by_ntp.find(ntp);
    if (it == _by_ntp.end()) {
        return;
    }
    _probe.increment_pushes();
    // Read the property directly rather than holding a binding: this is a
    // shard-local singleton member read (no atomics, no lock), and it keeps
    // the flag live without adding state to a class unit tests construct
    // directly. When off, the two clock reads below never happen at all.
    // Leading `::` is required, not stylistic: relay::service has a nested
    // `struct config`, so unqualified `config::` resolves to that instead of
    // the global config namespace.
    const bool measure
      = ::config::shard_local_cfg().relay_stage_metrics_enabled();
    auto started = measure ? ss::steady_clock_type::now()
                           : ss::steady_clock_type::time_point{};

    // Cross-shard transit, recorded BEFORE the delivery loop: what is being
    // measured is how long the record took to GET here, so it must not
    // include the local dispatch loop that fanout_duration already covers.
    // Unset dispatched_at means push()'s local path (no transit exists) or
    // stage metrics off at the producer.
    if (measure && dispatched_at != ss::steady_clock_type::time_point{}) {
        _probe.record_crossshard_transit(
          std::chrono::duration_cast<std::chrono::microseconds>(
            started - dispatched_at));
    }

    for (auto& [_, sub] : it->second) {
        // pushed_at (not dispatched_at) - the span being measured starts at the
        // producing transform's emit, so a local subscriber and one three shards
        // away are measured from the same instant.
        if (sub->deliver(data, pushed_at)) {
            _probe.increment_delivered();
        } else {
            _probe.increment_dropped();
        }
    }
    if (measure) {
        // The whole loop, not per subscriber: the cost being characterised is
        // that subscriber N waits for the N-1 synchronous deliver() calls
        // ahead of it, which is a property of the loop, and timing each
        // iteration would cost more than the thing being measured at low
        // fan-out. Recorded once per push per shard, so at fan-out 1000 this
        // is one sample covering 1000 deliveries - divide by
        // delivered_total's delta for a per-subscriber figure.
        _probe.record_fanout_duration(
          std::chrono::duration_cast<std::chrono::microseconds>(
            ss::steady_clock_type::now() - started));
    }
}

void service::push(const model::ntp& ntp, const iobuf& data) {
    // One clock read for the whole push, taken FIRST so every subscriber -
    // local and remote - is timed from the same emit instant.
    const bool measure_emit
      = ::config::shard_local_cfg().relay_stage_metrics_enabled();
    auto pushed_at = measure_emit ? ss::steady_clock_type::now()
                                  : ss::steady_clock_type::time_point{};
    deliver_locally(ntp, data, ss::steady_clock_type::time_point{}, pushed_at);
    auto it = _shards_with_subscribers.find(ntp);
    if (it == _shards_with_subscribers.end()) {
        return;
    }
    auto me = ss::this_shard_id();

    // Count the remote shards before allocating anything. When the only
    // subscribers are local - the common single-shard case, and fan-out 1 in
    // the benchmark - this function must stay exactly as cheap as it was
    // before: no copy, no allocation, no timing.
    size_t remote = 0;
    for (auto shard : it->second) {
        if (shard != me) {
            ++remote;
        }
    }
    if (remote == 0) {
        return;
    }

    const bool measure
      = ::config::shard_local_cfg().relay_stage_metrics_enabled();
    auto started = measure ? ss::steady_clock_type::now()
                           : ss::steady_clock_type::time_point{};

    // ONE copy for the entire fan-out, not one per shard.
    //
    // This used to be `[ntp, copy = data.copy()]` in each iteration's lambda,
    // which meant the PRODUCER's shard paid a full iobuf copy plus an ntp copy
    // (which owns strings) per remote shard, synchronously, before each
    // submission - and then the remote shard destroyed that copy, making every
    // one of them a cross-shard free as well. push() runs inside a wasm
    // transform's emit callback, so all of it sat in the producing matcher's
    // critical path. Measured 2026-08-29 (run_id=1788059806): the matcher's own
    // transform_e2e went from 498us at fan-out 1 to 162ms at fan-out 10 while
    // the relay's per-subscriber dispatch loop stayed at 25-100 NANOseconds -
    // i.e. essentially all of the fan-out cost was here, not in the relay's
    // delivery.
    //
    // Remote shards receive a plain read-only reference. Cross-shard *reads*
    // are legal in Seastar's single address space; what is not legal is
    // freeing another shard's memory or racing on it. Neither happens here:
    // the payload is fully written before any dispatch and never mutated
    // after, and it is owned by this shard for the whole fan-out.
    //
    // The lw_shared_ptrs are captured ONLY by the finally() continuation,
    // which is attached on - and therefore runs on - this shard.
    // ss::lw_shared_ptr's refcount is deliberately NON-ATOMIC, so copying or
    // destroying one on another shard would be a data race; the remote lambdas
    // capture raw pointers precisely to keep every refcount operation local.
    auto payload = ss::make_lw_shared<iobuf>(data.copy());
    auto topic = ss::make_lw_shared<model::ntp>(ntp);

    // Taken AFTER the payload copy and before the first submission, so
    // crossshard_transit measures the cross-shard path itself rather than
    // re-counting the copy that crossshard_dispatch already charges.
    //
    // One stamp for the whole loop, not one per shard. That means a later
    // shard's transit also includes the submission cost of the shards ahead of
    // it - bounded by (remote - 1) x per-submission cost, measured at 25-100ns,
    // so under ~2us even at fan-out 20. Deliberate: the effect being chased is
    // milliseconds, and taking one clock read per remote shard would multiply
    // the reads on this hot path by the fan-out factor, which is exactly the
    // kind of instrumentation that changes what it measures.
    auto dispatched_at = measure ? ss::steady_clock_type::now()
                                 : ss::steady_clock_type::time_point{};

    std::vector<ss::future<>> dispatched;
    dispatched.reserve(remote);
    for (auto shard : it->second) {
        if (shard == me) {
            continue;
        }
        dispatched.push_back(
          container()
            .invoke_on(
              shard,
              [t = payload.get(), n = topic.get(), dispatched_at, pushed_at](
                service& other) {
                  other.deliver_locally(*n, *t, dispatched_at, pushed_at);
              })
            .handle_exception(&ignore_no_sharded_instance));
    }

    if (measure) {
        // The producer-shard cost of fanning out: the single copy plus every
        // submission. Deliberately recorded BEFORE awaiting anything, because
        // what matters is how long the matcher was held up, not how long the
        // remote shards took to drain.
        _probe.record_crossshard_dispatch(
          std::chrono::duration_cast<std::chrono::microseconds>(
            ss::steady_clock_type::now() - started));
    }

    ssx::background = ss::when_all(dispatched.begin(), dispatched.end())
                        .discard_result()
                        .finally([payload, topic] {});
}

uint32_t service::add_subscription(model::ntp ntp, subscription* sub) {
    auto id = _next_id++;
    auto& subs = _by_ntp[ntp];
    subs.emplace(id, sub);
    bool first_local_subscriber = subs.size() == 1;
    // No other shards to reach (or, in unit tests, no sharded<> container to
    // broadcast through at all - relay::service is often constructed
    // directly there, bypassing sharded<> entirely). Either way, nothing to
    // do: push() only consults _shards_with_subscribers, which stays
    // correctly empty.
    if (first_local_subscriber && ss::this_smp_shard_count() > 1) {
        auto me = ss::this_shard_id();
        ssx::background = container()
                            .invoke_on_all([ntp, me](service& other) {
                                other._shards_with_subscribers[ntp].insert(me);
                            })
                            .handle_exception(&ignore_no_sharded_instance);
    }
    _ntp_by_id.emplace(id, std::move(ntp));
    _probe.subscription_added();
    return id;
}

void service::remove_subscription(uint32_t id) {
    auto ntp_it = _ntp_by_id.find(id);
    if (ntp_it == _ntp_by_id.end()) {
        return;
    }
    auto ntp = ntp_it->second;
    auto it = _by_ntp.find(ntp);
    bool last_local_subscriber = false;
    if (it != _by_ntp.end()) {
        it->second.erase(id);
        if (it->second.empty()) {
            _by_ntp.erase(it);
            last_local_subscriber = true;
        }
    }
    _ntp_by_id.erase(ntp_it);
    _probe.subscription_removed();
    if (last_local_subscriber && ss::this_smp_shard_count() > 1) {
        auto me = ss::this_shard_id();
        ssx::background
          = container()
              .invoke_on_all([ntp, me](service& other) {
                  auto sit = other._shards_with_subscribers.find(ntp);
                  if (sit == other._shards_with_subscribers.end()) {
                      return;
                  }
                  sit->second.erase(me);
                  if (sit->second.empty()) {
                      other._shards_with_subscribers.erase(sit);
                  }
              })
              .handle_exception(&ignore_no_sharded_instance);
    }
}

size_t service::subscription_count(const model::ntp& ntp) const {
    auto it = _by_ntp.find(ntp);
    return it == _by_ntp.end() ? 0 : it->second.size();
}

} // namespace relay
