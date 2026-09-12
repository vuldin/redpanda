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

#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/transform.h"
#include "ssx/work_queue.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_map.h>

#include <chrono>
#include <type_traits>
#include <vector>

namespace transform {

/**
 * The compiled-module cache, as residency needs to see it.
 *
 * An interface rather than wasm::caching_runtime directly, for the same
 * reason registry and processor_factory are interfaces: the only place that
 * can hand out the real one is transform::service, which no test target
 * links, so anything worth testing has to sit behind something a test can
 * substitute.
 */
class factory_store {
public:
    factory_store() = default;
    factory_store(const factory_store&) = delete;
    factory_store& operator=(const factory_store&) = delete;
    factory_store(factory_store&&) = delete;
    factory_store& operator=(factory_store&&) = delete;
    virtual ~factory_store() = default;

    /**
     * Keep this transform's module in memory past its last processor,
     * compiling it now if it is not compiled already.
     *
     * False means the module is not resident: the bound was reached and
     * nothing could be given up, or the binary could not be fetched. Neither
     * is an error - residency is speculative and the transform runs either
     * way - so the caller simply tries again on a later sweep.
     */
    virtual ss::future<bool> pin(model::transform_metadata) = 0;

    /// Stop keeping this module in memory. Safe for one that is not held.
    virtual void unpin(model::offset) = 0;

    /// The offsets currently held, so a sweep can diff against what it wants.
    virtual std::vector<model::offset> resident() const = 0;

    /**
     * True while a fetch or a compile is in flight anywhere on this broker.
     *
     * Every compile in the process runs on one alien thread, so pre-warming
     * while this is true would queue in front of a transform that is
     * actually waiting to start - making a real cold start slower in order to
     * avoid a hypothetical one.
     */
    virtual bool busy() const = 0;

    /// How many modules may be held at once. Zero disables residency.
    virtual void set_max_resident(size_t) = 0;
};

/**
 * Keeps a transform's compiled module in memory on every broker that
 * replicates its input topic, so the broker that takes a partition over does
 * not have to fetch and compile the module at the moment it takes over.
 *
 * Draining makes a planned handover clean, but it does nothing about the
 * receiving end: the new owner still pays the fetch and the compile, and pays
 * them exactly when it becomes responsible for the partition. That cost is
 * why this exists.
 *
 * Keyed on the deploy offset, like the cache itself, and deliberately not per
 * partition: holding one local replica is the entire justification for
 * keeping a module, so a per-partition key would refcount a distinction that
 * does not change the answer.
 *
 * Declarative rather than event-driven. Every sweep derives what belongs here
 * and fixes the difference against what is held, so each transition falls out
 * of the diff instead of needing its own handler - a redeploy moves
 * source_ptr, so the old module is given back and the new one taken; a delete
 * drops out of the transform list; a replica moving away drops out of the
 * wanted set; and the bound going to zero gives everything back. It also
 * means a notification is only ever a hint to re-derive, never the source of
 * truth, which is what makes one that is missed, duplicated or out of order
 * harmless.
 */
template<typename ClockType = ss::lowres_clock>
class factory_residency {
    static_assert(
      std::is_same_v<ClockType, ss::lowres_clock>
        || std::is_same_v<ClockType, ss::manual_clock>,
      "Only lowres_clock and manual_clock are supported");

public:
    using transform_map
      = absl::btree_map<model::transform_id, model::transform_metadata>;
    /// Every transform this broker knows about. Read per sweep, so a
    /// transform deployed after start-up needs no extra bookkeeping.
    using transforms_fn = ss::noncopyable_function<transform_map()>;
    /**
     * Whether this broker holds a replica of any partition of this topic.
     *
     * Replica rather than leadership, because the replica set strictly
     * contains the leader: a broker that leads also replicates, so it is
     * held either way, and a leader that flaps between two brokers stays
     * warm on both rather than only where it currently sits.
     */
    using replicates_fn
      = ss::noncopyable_function<bool(const model::topic_namespace&)>;

    factory_residency(
      factory_store*, config::binding<size_t>, transforms_fn, replicates_fn);
    factory_residency(const factory_residency&) = delete;
    factory_residency& operator=(const factory_residency&) = delete;
    factory_residency(factory_residency&&) = delete;
    factory_residency& operator=(factory_residency&&) = delete;
    ~factory_residency() = default;

    /// Pushes the current bound to the store and starts sweeping. Until this
    /// is called nothing is held.
    void start();
    /// Waits for an in-flight sweep. Safe to call without start().
    ss::future<> stop();

    /**
     * Something changed that may have altered which modules belong here.
     *
     * Cheap and idempotent, because its caller is a partition notification: a
     * burst collapses into a single sweep, which is what makes the boot-time
     * replay of every existing partition affordable.
     */
    void poke();

    /**
     * Take what belongs here, give back what does not.
     *
     * Public because it is the whole behaviour, and driving it only through
     * notifications and timers would leave it testable only by timing.
     */
    ss::future<> reconcile();

    /// Completed sweeps, so a test can wait on progress rather than sleeping.
    size_t reconcile_count() const { return _reconciles; }

private:
    /**
     * How long a poke waits before sweeping.
     *
     * A leadership move hands several partitions over at once and each of
     * them pokes, so the settle both collapses that burst and - more to the
     * point - puts any speculative compile behind the real cold starts that
     * the very same burst just triggered.
     */
    static constexpr
      typename ClockType::duration settle_delay = std::chrono::seconds(10);
    /**
     * Backstop for anything no notification covers: a replica this broker
     * acquired while it was not running, or a notification dropped. Long,
     * because its job is to be eventually correct rather than timely.
     */
    static constexpr
      typename ClockType::duration resweep_interval = std::chrono::minutes(5);

    void arm_resweep();

    factory_store* _store;
    config::binding<size_t> _max_resident;
    transforms_fn _transforms;
    replicates_fn _replicates;
    // One sweep at a time, so two of them cannot both decide to compile.
    ssx::work_queue _queue;
    // Set while a sweep is scheduled but has not run yet. This is what
    // collapses a burst of pokes into one sweep.
    bool _sweep_scheduled = false;
    size_t _reconciles = 0;
    ss::timer<ClockType> _resweep_timer;
    ss::gate _gate;
};

} // namespace transform
