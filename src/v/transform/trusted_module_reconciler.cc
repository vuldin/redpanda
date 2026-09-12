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
#include "transform/trusted_module_reconciler.h"

#include "config/configuration.h"
#include "ssx/future-util.h"
#include "transform/logger.h"

#include <seastar/coroutine/maybe_yield.hh>

namespace transform {

trusted_module_reconciler::trusted_module_reconciler(
  transforms_fn transforms, rebuild_fn rebuild)
  : _binding(config::shard_local_cfg().wasm_trusted_modules.bind())
  , _applied(_binding())
  , _transforms(std::move(transforms))
  , _rebuild(std::move(rebuild)) {}

void trusted_module_reconciler::start() {
    _binding.watch([this] {
        // A config watch callback is synchronous and reconciling drains
        // processors, so it cannot run inline.
        ssx::spawn_with_gate(_gate, [this] {
            return reconcile().handle_exception(
              [](const std::exception_ptr& e) {
                  // Logged, not propagated: this runs on the config-update
                  // path, and failing there would affect every other
                  // property's update too.
                  vlog(
                    tlog.warn, "reapplying wasm_trusted_modules failed: {}", e);
              });
        });
    });
}

ss::future<> trusted_module_reconciler::stop() { return _gate.close(); }

ss::future<> trusted_module_reconciler::reconcile() {
    // Snapshot before rebuilding anything. A second edit arriving while this
    // one is still working must diff against what the engines were actually
    // built with, not against a value no engine ever saw.
    auto previous = std::exchange(_applied, _binding());

    for (const auto& [id, meta] : _transforms()) {
        if (meta.binary_sha256.empty()) {
            // No recorded digest, so it could never have matched the
            // allowlist and cannot have lost anything.
            continue;
        }
        if (!config::trusted_grant_changed(
              previous, _applied, meta.binary_sha256)) {
            continue;
        }
        vlog(
          tlog.info,
          "wasm_trusted_modules changed for transform {}; rebuilding it so "
          "the change takes effect",
          id);
        co_await _rebuild(id);
        co_await ss::coroutine::maybe_yield();
    }
    ++_reconciles;
}

} // namespace transform
