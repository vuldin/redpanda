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
#include "metrics/metrics.h"
#include "model/transform.h"
#include "ssx/mutex.h"
#include "wasm/engine.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <optional>
#include <vector>

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
     * Discard the cached factory for this deploy offset, so the next caller
     * compiles a fresh one instead of reusing it.
     *
     * Exists because a factory's capability grants are baked in when it is
     * compiled, so revoking a grant has no effect on a module that is already
     * compiled. Dropping the cache entry is what makes the next compile pick
     * the revocation up. Engines already built keep running on the old
     * factory - they hold their own strong reference - which is deliberate:
     * tearing them down here would turn a capability change into an
     * unannounced data-plane interruption. What matters is that a processor
     * RESTART does not silently inherit the old grants.
     *
     * Safe to call for an offset that is not cached, since the caller is a
     * config watch that does not know what this broker has compiled.
     */
    void invalidate_factory(model::offset);

    /**
     * Keep this transform's compiled module in memory even while no processor
     * is using it, so that a processor starting later - typically the one on
     * the far side of a leadership move - finds it already compiled.
     *
     * Every entry in the module cache is a weak reference, so today a module
     * dies with the last processor that used it and the next start pays the
     * fetch and the compile again. That is the bulk of a cold start, and it
     * is paid on the broker that just became responsible for the partition,
     * at the moment it became responsible.
     *
     * Bounded by set_max_resident_factories(), and the bound counts modules
     * rather than bytes because it has to be applied BEFORE the loader runs:
     * a module's size is only known once it is compiled
     * (factory::image_bytes), so a byte bound would have to fetch and compile
     * in order to discover that it must refuse - spending the exact resource
     * the bound exists to protect, on every sweep, forever. The byte figure
     * is reported as a gauge instead, so the count can be sized against real
     * binaries.
     *
     * Returns false when the pin was refused, when the binary could not be
     * loaded, or when the pin was withdrawn or invalidated while the module
     * was being compiled. None of those are errors: the caller is
     * speculative, and the transform runs either way.
     */
    ss::future<bool> pin_factory(model::transform_metadata, binary_loader_fn);

    /**
     * Stop keeping this module in memory. The module still survives while a
     * processor is using it - this only drops the reference that outlives
     * them.
     *
     * Safe for an offset that is not pinned, since a caller reconciling
     * against cluster state does not track what it has previously asked for.
     */
    void unpin_factory(model::offset);

    /**
     * Whether this offset is being kept resident. True from the moment a pin
     * is admitted, so it covers a module that is still being compiled - which
     * is what a caller reconciling towards a desired set wants, since asking
     * again would be a no-op.
     */
    bool is_resident(model::offset) const;

    /**
     * The offsets currently kept resident, ascending, for a caller to diff
     * against the set it wants.
     */
    std::vector<model::offset> resident_factories() const;

    /**
     * How many modules may be kept resident at once. Zero disables residency.
     *
     * Lowering this releases modules immediately rather than waiting for them
     * to be invalidated, so that turning residency down - or off - actually
     * returns the memory.
     */
    void set_max_resident_factories(size_t);

    struct residency_stats {
        // Slots in use, including a module still being compiled.
        size_t count = 0;
        // Executable memory held by the modules that are compiled. A slot
        // whose compile has not finished contributes nothing yet.
        size_t image_bytes = 0;
        // Monotonic count of pins refused for want of a free slot.
        uint64_t admission_failures = 0;
    };
    residency_stats residency() const;

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

    /**
     * The cache lookup without the side effect.
     *
     * get_cached_factory stamps the entry as used, because every caller of it
     * is about to build an engine from what it gets back. Residency's own
     * bookkeeping must not do that, or the module it just pinned would look
     * like one a transform had asked for and stop being the first thing
     * evicted.
     */
    ss::optimized_optional<ss::shared_ptr<factory>> find_cached(model::offset);

    /**
     * Drop the oldest resident module that no transform has ever asked for,
     * returning false if every resident module has been used.
     *
     * Refusing a new pin rather than evicting a module in use is deliberate.
     * With N+1 transforms competing for N slots, evicting the one being used
     * means it recompiles the next time its partition moves AND the newcomer
     * that displaced it never gets used either - a loop of cold starts on the
     * single thread that compiles, which is worse than not pre-warming the
     * newcomer at all.
     */
    bool evict_unused();

    void register_metrics();

    struct resident_entry {
        // The strong reference that is the entire point: _factory_cache holds
        // only weak ones. Null while the module is still being fetched and
        // compiled - the entry is created the moment the pin is admitted, so
        // the bound is decided once, before any work, and a pin that suspends
        // cannot let a concurrent one overshoot it.
        ss::shared_ptr<factory> strong;
        size_t image_bytes = 0;
        ss::lowres_clock::time_point pinned_at{};
        // When a caller last took this module out of the cache in order to
        // build an engine from it. Default-constructed means residency has
        // never paid off here, which is what makes it the first candidate for
        // eviction.
        ss::lowres_clock::time_point last_used_at{};
    };

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
    // Bumped by every invalidate_factory. A creation in flight captures this
    // before it suspends and re-checks it before caching, so an invalidation
    // that lands mid-fetch or mid-compile is not immediately undone by the
    // insert of a factory that was linked under the grants being revoked.
    uint64_t _cache_epoch = 0;
    absl::btree_map<model::offset, resident_entry> _resident;
    size_t _max_resident_factories = 0;
    uint64_t _resident_admission_failures = 0;
    ss::sharded<engine_cache> _engine_caches;
    ss::lowres_clock::duration _gc_interval;
    ss::timer<ss::lowres_clock> _gc_timer;
    ss::gate _gate;
    metrics::public_metric_groups _public_metrics;
};

} // namespace wasm
