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
#include "config/wasm_trusted_module.h"
#include "model/transform.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_map.h>

#include <vector>

namespace transform {

/**
 * Keeps running transforms in step with config::wasm_trusted_modules.
 *
 * The wasm engine reads the allowlist once, in its constructor, so a running
 * instance keeps whatever capabilities it was granted at start-up. Removing an
 * entry therefore does nothing until that transform is rebuilt - while the
 * property is needs_restart::no, which tells an operator the change is live.
 * Closing that gap is this class's whole purpose, and the direction that
 * matters is revocation.
 *
 * It exists as its own type, rather than as a few members on
 * transform::service, so that it can be tested. The service needs a dozen
 * sharded dependencies to construct and nothing in the tree builds one, so
 * anything living there is reachable only by inspection - and the parts most
 * worth testing here are exactly the ones inspection is worst at: whether the
 * watch is installed at all, whether the snapshot is taken at the right
 * moment, and whether every affected transform is dispatched rather than just
 * the first.
 *
 * Both collaborators are injected for the same reason: a test supplies a
 * transform list and records rebuilds, with no cluster involved.
 */
class trusted_module_reconciler {
public:
    using transform_map
      = absl::btree_map<model::transform_id, model::transform_metadata>;
    /// Every transform this shard knows about. Called per reconcile, so a
    /// transform deployed after start-up is seen without extra bookkeeping.
    using transforms_fn = ss::noncopyable_function<transform_map()>;
    /// Drain and rebuild one transform, so it comes back under the new grant.
    using rebuild_fn
      = ss::noncopyable_function<ss::future<>(model::transform_id)>;

    trusted_module_reconciler(transforms_fn, rebuild_fn);

    /// Installs the config watch. Until this is called a change to the
    /// allowlist has no effect on anything already running.
    void start();
    /// Waits for an in-flight reconcile. Safe to call without start().
    ss::future<> stop();

    /**
     * Rebuild every transform whose binary's grant changed since the last
     * reconcile.
     *
     * Public because it is the whole behaviour, and testing it through a
     * config change alone would leave the dispatch untestable except by
     * timing.
     */
    ss::future<> reconcile();

    /// Completed reconciles, so a test can wait for the watch to have fired
    /// rather than sleeping.
    size_t reconcile_count() const { return _reconciles; }

private:
    config::binding<std::vector<config::wasm_trusted_module>> _binding;
    // What the running engines were built against. Compared against, and
    // replaced by, the binding's current value on each reconcile.
    std::vector<config::wasm_trusted_module> _applied;
    transforms_fn _transforms;
    rebuild_fn _rebuild;
    size_t _reconciles = 0;
    ss::gate _gate;
};

} // namespace transform
