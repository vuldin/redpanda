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

#include "transform/factory_residency.h"

#include "transform/logger.h"
#include "transform/relay_source_env.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>

#include <absl/container/flat_hash_set.h>

namespace transform {

namespace {

/**
 * A relay-sourced transform pinned to a shard is created and kept alive on
 * that shard regardless of leadership, so its own processor already holds its
 * module for as long as the transform exists. Holding a second reference
 * would spend a slot on the one module that cannot go cold.
 */
bool keeps_its_own_module_alive(const model::transform_metadata& meta) {
    return is_relay_sourced(meta) && relay_target_shard(meta).has_value();
}

} // namespace

template<typename ClockType>
factory_residency<ClockType>::factory_residency(
  factory_store* store,
  config::binding<size_t> max_resident,
  transforms_fn transforms,
  replicates_fn replicates)
  : _store(store)
  , _max_resident(std::move(max_resident))
  , _transforms(std::move(transforms))
  , _replicates(std::move(replicates))
  , _queue([](const std::exception_ptr& e) {
      vlog(tlog.warn, "failed keeping wasm modules resident: {}", e);
  })
  , _resweep_timer([this] {
      poke();
      arm_resweep();
  }) {}

template<typename ClockType>
void factory_residency<ClockType>::start() {
    _store->set_max_resident(_max_resident());
    _max_resident.watch([this] {
        // The bound goes to the store before the sweep is scheduled, so
        // lowering it releases modules now rather than whenever the sweep
        // gets around to them, and raising it is already in effect when the
        // sweep decides what it is allowed to take.
        _store->set_max_resident(_max_resident());
        poke();
    });
    arm_resweep();
    poke();
}

template<typename ClockType>
void factory_residency<ClockType>::arm_resweep() {
    _resweep_timer.arm(ClockType::now() + resweep_interval);
}

template<typename ClockType>
ss::future<> factory_residency<ClockType>::stop() {
    _resweep_timer.cancel();
    co_await _queue.shutdown();
    co_await _gate.close();
}

template<typename ClockType>
void factory_residency<ClockType>::poke() {
    if (_sweep_scheduled) {
        return;
    }
    _sweep_scheduled = true;
    _queue.template submit_delayed<ClockType>(settle_delay, [this] {
        // Cleared before the sweep rather than after it, so a change that
        // lands while this sweep is running schedules the next one instead of
        // being swallowed by it.
        _sweep_scheduled = false;
        return reconcile();
    });
}

template<typename ClockType>
ss::future<> factory_residency<ClockType>::reconcile() {
    auto holder = _gate.hold();
    auto resident = _store->resident();

    if (_max_resident() == 0) {
        // Disabled, or just turned off. Give everything back rather than
        // holding it until the process restarts.
        for (auto offset : resident) {
            _store->unpin(offset);
        }
        ++_reconciles;
        co_return;
    }

    absl::btree_map<model::offset, model::transform_metadata> wanted;
    for (const auto& entry : _transforms()) {
        const auto& meta = entry.second;
        if (meta.paused) {
            // Nothing is going to start it, so there is no cold start here to
            // avoid.
            continue;
        }
        if (keeps_its_own_module_alive(meta)) {
            continue;
        }
        if (!_replicates(meta.input_topic)) {
            continue;
        }
        wanted.insert_or_assign(meta.source_ptr, meta);
    }

    for (auto offset : resident) {
        if (!wanted.contains(offset)) {
            _store->unpin(offset);
        }
    }

    absl::flat_hash_set<model::offset> held(resident.begin(), resident.end());
    for (const auto& entry : wanted) {
        if (held.contains(entry.first)) {
            continue;
        }
        if (_store->busy()) {
            // A transform is waiting on the one thread that compiles.
            // Queueing behind it would delay a cold start that is actually
            // happening in order to pre-empt one that might never happen.
            // Whatever is left is picked up by a later sweep.
            break;
        }
        co_await _store->pin(entry.second);
    }

    ++_reconciles;
}

template class factory_residency<ss::lowres_clock>;
template class factory_residency<ss::manual_clock>;

} // namespace transform
