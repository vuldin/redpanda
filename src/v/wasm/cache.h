/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/container/btree_map.h"
#include "model/transform.h"
#include "ssx/mutex.h"
#include "wasm/engine.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <optional>

namespace wasm {

class engine_cache;
class cached_factory;

/**
 * A runtime that reuses factories and caches them per process as to share the
 * executable memory.
 *
 * To enable this, this runtime can only create factories on a single shard
 * (the same shard that it is created on, which is probably shard zero).
 * However, factories that are created by this runtime can be used to create
 * engines for any shard.
 *
 * Additionally, engines from this runtime's factories are reused within a
 * single shard. Ramifications of this is that failures to a single engine cause
 * the engine to be restarted and all users of a given engine must wait until
 * it's restarted to use the engine.
 */
class caching_runtime : public runtime {
public:
    explicit caching_runtime(std::unique_ptr<runtime>);
    caching_runtime(
      std::unique_ptr<runtime>, ss::lowres_clock::duration gc_interval);
    caching_runtime(const caching_runtime&) = delete;
    caching_runtime(caching_runtime&&) = delete;
    caching_runtime& operator=(const caching_runtime&) = delete;
    caching_runtime& operator=(caching_runtime&&) = delete;
    ~caching_runtime() override;

    ss::future<> start(runtime::config) override;
    ss::future<> stop() override;

    /**
     * Create a factory, must be called only on a single shard.
     */
    ss::future<ss::shared_ptr<factory>> make_factory(
      model::transform_metadata, model::wasm_binary_iobuf) override;

    /**
     * If a factory exists in cached, return it without needing the binary.
     */
    ss::optimized_optional<ss::shared_ptr<factory>>
    get_cached_factory(const model::transform_metadata&);

    /**
     * Fetches the binary only if this call is the one that will compile it.
     *
     * make_factory takes the binary as an argument, so its caller has to
     * fetch before it can find out whether a compile was needed at all. With
     * N partitions of one transform arriving on a broker together - which is
     * the normal case after a leadership move - that is N fetches of the same
     * bytes over RPC, of which N-1 are then discarded by the cache check.
     * Only the compile was ever deduplicated.
     *
     * Taking the loader as a callback instead lets the fetch happen inside
     * the per-offset lock, after the second cache check, so it runs at most
     * once per compile. It also widens is_creating_factory() to cover the
     * fetch window, not just the compile.
     *
     * The loader returns nullopt to mean "could not fetch"; this returns a
     * null factory in that case, and caches nothing, so the next caller
     * retries rather than inheriting the failure.
     */
    using binary_loader_fn = ss::noncopyable_function<
      ss::future<std::optional<model::wasm_binary_iobuf>>()>;
    ss::future<ss::shared_ptr<factory>>
      get_or_create_factory(model::transform_metadata, binary_loader_fn);

    /**
     * True while some caller holds a factory-creation lock, i.e. a fetch or a
     * compile is in flight somewhere on this broker.
     *
     * Every compile in the process runs on one alien thread, so this exists
     * for speculative work to check before queueing in front of a compile
     * that a running transform is actually blocked on.
     */
    bool is_creating_factory() const {
        return !_factory_creation_mu_map.empty();
    }

    ss::future<> validate(model::wasm_binary_iobuf) override;

private:
    friend class WasmCacheTest;

    /**
     * GC factories and engines that are no longer in use.
     *
     * Return the number of entries deleted (for testing).
     */
    ss::future<int64_t> do_gc();
    ss::future<int64_t> gc_factories();
    ss::future<int64_t> gc_engines();

    /*
     * This map holds locks for creating factories.
     *
     * These mutexes are shortlived and should only live during the creation of
     * factories.
     */
    absl::btree_map<model::offset, std::unique_ptr<ssx::mutex>>
      _factory_creation_mu_map;
    std::unique_ptr<runtime> _underlying;
    absl::btree_map<model::offset, ss::weak_ptr<cached_factory>> _factory_cache;
    ss::sharded<engine_cache> _engine_caches;
    ss::lowres_clock::duration _gc_interval;
    ss::timer<ss::lowres_clock> _gc_timer;
    ss::gate _gate;
};

} // namespace wasm
