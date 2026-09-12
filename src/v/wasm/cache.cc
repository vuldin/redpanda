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

#include "wasm/cache.h"

#include "logger.h"
#include "metrics/prometheus_sanitize.h"
#include "model/transform.h"
#include "ssx/future-util.h"
#include "wasm/wasi_logger.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/optimized_optional.hh>

#include <algorithm>

namespace wasm {

namespace {

/**
 * The interval at which we gc factories and engines that are no longer used.
 */
constexpr auto default_gc_interval = std::chrono::minutes(10);

template<typename Key, typename Value>
ss::future<int64_t>
gc_btree_map(absl::btree_map<Key, ss::weak_ptr<Value>>* cache) {
    int64_t cleanup_count = 0;
    auto it = cache->begin();
    while (it != cache->end()) {
        // If the weak_ptr is `nullptr` then we can remove it from the cache.
        if (!it->second) {
            ++cleanup_count;
            it = cache->erase(it);
        } else {
            ++it;
        }

        if (ss::need_preempt() && it != cache->end()) {
            // The iterator could have be invalidated if there was a write
            // during the yield. We'll use the ordered nature of the btree to
            // support resuming the iterator after the suspension point.
            Key checkpoint = it->first;
            co_await ss::yield();
            it = cache->lower_bound(checkpoint);
        }
    }
    co_return cleanup_count;
}

/**
 * Allows sharing an engine between multiple uses.
 *
 * Must live on a single core.
 */
class shared_engine
  : public engine
  , public ss::enable_shared_from_this<shared_engine>
  , public ss::weakly_referencable<shared_engine> {
public:
    explicit shared_engine(
      ss::shared_ptr<engine> underlying,
      ss::foreign_ptr<ss::shared_ptr<factory>> f)
      : _underlying(std::move(underlying))
      , _factory(std::move(f)) {}

    ss::future<> transform(
      model::record_batch batch,
      transform_probe* probe,
      transform_callback cb) override {
        auto u = co_await _mu.get_units();
        auto fut = co_await ss::coroutine::as_future(
          _underlying->transform(std::move(batch), probe, std::move(cb)));
        if (!fut.failed()) {
            co_return;
        }
        // Restart the engine
        try {
            co_await _underlying->stop();
            co_await _underlying->start();
        } catch (...) {
            vlog(
              wasm_log.warn,
              "failed to restart wasm engine: {}",
              std::current_exception());
        }
        std::rethrow_exception(fut.get_exception());
    }

    ss::future<> start() override {
        auto u = co_await _mu.get_units();
        if (_ref_count++ == 0) {
            co_await _underlying->start();
        }
    }
    ss::future<> stop() override {
        vassert(
          _ref_count > 0, "expected a call to start before a call to stop");
        auto u = co_await _mu.get_units();
        if (--_ref_count == 0) {
            co_await _underlying->stop();
        }
    }

    ~shared_engine() override {
        vassert(
          _ref_count == 0, "expected engine to be stopped before destruction");
    }

private:
    ssx::mutex _mu{"wasm_shared_engine"};
    size_t _ref_count = 0;
    ss::shared_ptr<engine> _underlying;
    // This factory reference is here to keep the cache entry alive.
    ss::foreign_ptr<ss::shared_ptr<factory>> _factory;
};

/**
 * A RAII scoped lock that ensures factory locks are deleted when there are no
 * waiters.
 */
class factory_creation_lock_guard {
public:
    factory_creation_lock_guard(const factory_creation_lock_guard&) = delete;
    factory_creation_lock_guard&
    operator=(const factory_creation_lock_guard&) = delete;
    factory_creation_lock_guard(factory_creation_lock_guard&&) noexcept
      = default;
    factory_creation_lock_guard&
    operator=(factory_creation_lock_guard&&) noexcept = default;

    static ss::future<factory_creation_lock_guard> acquire(
      absl::btree_map<model::offset, std::unique_ptr<ssx::mutex>>* mu_map,
      model::offset offset) {
        auto it = mu_map->find(offset);
        ssx::mutex* mu = nullptr;
        if (it == mu_map->end()) {
            auto inserted = mu_map->emplace(
              offset,
              std::make_unique<ssx::mutex>("factory_creation_lock_guard"));
            vassert(inserted.second, "expected mutex to be inserted");
            mu = inserted.first->second.get();
        } else {
            mu = it->second.get();
        }
        ssx::mutex::units units = co_await mu->get_units();
        co_return factory_creation_lock_guard(
          offset, mu_map, mu, std::move(units));
    }

    ~factory_creation_lock_guard() {
        _underlying.return_all();
        // If nothing is waiting on or holding the mutex, we can remove the lock
        // from the map.
        if (_mu->ready()) {
            _mu_map->erase(_offset);
        }
    }

private:
    factory_creation_lock_guard(
      model::offset offset,
      absl::btree_map<model::offset, std::unique_ptr<ssx::mutex>>* mu_map,
      ssx::mutex* mu,
      ssx::mutex::units underlying)
      : _offset(offset)
      , _mu_map(mu_map)
      , _mu(mu)
      , _underlying(std::move(underlying)) {}

    model::offset _offset;
    absl::btree_map<model::offset, std::unique_ptr<ssx::mutex>>* _mu_map;
    ssx::mutex* _mu;
    ssx::mutex::units _underlying;
};
} // namespace

/**
 * A cache for engines on a particular core.
 *
 * Keyed on a transform's deploy offset together with the specific partition
 * (ntp) an engine processes, so that each partition of a transform led on
 * this shard gets its own engine instance and linear memory rather than
 * sharing (and serializing on) one.
 */
class engine_cache {
public:
    using key_type = std::pair<model::offset, model::ntp>;

    void put(key_type key, const ss::shared_ptr<shared_engine>& engine) {
        _cache.insert_or_assign(std::move(key), engine->weak_from_this());
    }

    ss::future<ssx::mutex::units> lock() { return _mu.get_units(); }

    ss::optimized_optional<ss::shared_ptr<engine>> get(const key_type& key) {
        auto it = _cache.find(key);
        if (it == _cache.end() || !it->second) {
            return {};
        }
        return ss::static_pointer_cast<engine>(it->second->shared_from_this());
    }

    ss::future<int64_t> gc() { return gc_btree_map(&_cache); }

private:
    ssx::mutex _mu{"wasm_engine_cache"};
    absl::btree_map<key_type, ss::weak_ptr<shared_engine>> _cache;
};

/**
 * A factory
 *
 * Owned by a single core (shared zero) but can be used on any core to make an
 * engine local to that core.
 */
class cached_factory
  : public factory
  , public ss::enable_shared_from_this<cached_factory>
  , public ss::weakly_referencable<cached_factory> {
public:
    cached_factory(
      ss::foreign_ptr<ss::shared_ptr<factory>> f,
      model::offset offset,
      ss::sharded<engine_cache>* e,
      size_t image_bytes)
      : _offset(offset)
      , _underlying(std::move(f))
      , _engine_cache(e)
      , _image_bytes(image_bytes) {}

    ss::future<ss::shared_ptr<engine>>
    make_engine(model::ntp ntp, std::unique_ptr<wasm::logger> logger) override {
        engine_cache::key_type key{_offset, ntp};
        auto engine = _engine_cache->local().get(key);
        // Try to grab an engine outside the lock
        if (engine) {
            co_return *std::move(engine);
        }
        // Acquire the lock for this core
        auto u = co_await _engine_cache->local().lock();
        // Double check nobody created one while we were grabbing the lock.
        engine = _engine_cache->local().get(key);
        if (engine) {
            co_return *std::move(engine);
        }
        // Create the actual engine and put it in the cache.
        //
        // The multiplexing engine keeps a foreign reference to this factory
        // because the factory exists only on a single shard and nothing is
        // expected to keep a reference to a factory after the engine is
        // created.
        auto foreign_this = co_await foreign_from_this();
        auto created = ss::make_shared<shared_engine>(
          co_await _underlying->make_engine(std::move(ntp), std::move(logger)),
          std::move(foreign_this));
        _engine_cache->local().put(std::move(key), created);
        co_return created;
    }

    ss::future<ss::foreign_ptr<ss::shared_ptr<factory>>> foreign_from_this() {
        return ss::smp::submit_to(_underlying.get_owner_shard(), [this] {
            return ss::make_foreign<ss::shared_ptr<factory>>(
              shared_from_this());
        });
    }

    // Copied from the wrapped factory at construction time, on the shard that
    // owns it, so that reporting it never reaches across a foreign_ptr.
    size_t image_bytes() const final { return _image_bytes; }

private:
    model::offset _offset;
    ss::foreign_ptr<ss::shared_ptr<factory>> _underlying;
    ss::sharded<engine_cache>* _engine_cache;
    size_t _image_bytes;
};

caching_runtime::caching_runtime(std::unique_ptr<runtime> u)
  : caching_runtime(std::move(u), default_gc_interval) {}

caching_runtime::caching_runtime(
  std::unique_ptr<runtime> u, ss::lowres_clock::duration gc_interval)
  : _underlying(std::move(u))
  , _gc_interval(gc_interval)
  , _gc_timer([this]() {
      ssx::spawn_with_gate(_gate, [this] { return do_gc().discard_result(); });
  }) {}

caching_runtime::~caching_runtime() = default;

ss::future<> caching_runtime::start(runtime::config c) {
    co_await _underlying->start(c);
    co_await _engine_caches.start();
    register_metrics();
    _gc_timer.arm(_gc_interval);
}

ss::future<> caching_runtime::stop() {
    _gc_timer.cancel();
    _public_metrics.clear();
    co_await _gate.close();
    // Before _engine_caches.stop(), because a cached_factory holds a raw
    // pointer to that sharded service. Nothing reaches a resident module
    // after this point anyway, but the ordering is what makes that true
    // rather than lucky.
    _resident.clear();
    co_await _engine_caches.stop();
    co_await _underlying->stop();
}

ss::future<ss::shared_ptr<factory>> caching_runtime::make_factory(
  model::transform_metadata meta, model::wasm_binary_iobuf binary) {
    // Delegates so there is exactly ONE place that compiles and caches, and
    // therefore exactly one place that has to honour the invalidation epoch.
    // A second insert site would be a silent hole in that guard.
    //
    // The caller already holds the binary, so the "loader" just hands it
    // over - which also means a caller that loses the race for the lock
    // discards the binary it brought, exactly as before.
    auto held = ss::make_lw_shared<std::optional<model::wasm_binary_iobuf>>(
      std::move(binary));
    return get_or_create_factory(
      std::move(meta),
      [held]() -> ss::future<std::optional<model::wasm_binary_iobuf>> {
          return ss::make_ready_future<std::optional<model::wasm_binary_iobuf>>(
            std::move(*held));
      });
}

ss::future<ss::shared_ptr<factory>> caching_runtime::get_or_create_factory(
  model::transform_metadata meta, binary_loader_fn load) {
    model::offset offset = meta.source_ptr;
    // Captured before ANY suspension point, not just before the compile.
    // Both the fetch and the compile can be in flight when a capability
    // change invalidates this offset, and a factory produced by either was
    // linked under the grants being revoked - so capturing after the fetch
    // would miss an invalidation that arrived during it, which is the longer
    // of the two windows.
    auto epoch_at_start = _cache_epoch;
    // Outside the lock, so the common hit costs nothing.
    auto cached = get_cached_factory(meta);
    if (cached) {
        co_return *cached;
    }
    auto lock = co_await factory_creation_lock_guard::acquire(
      &_factory_creation_mu_map, offset);
    // Again under the lock: whoever held it before us may have created it,
    // and this is the check that makes the fetch below happen at most once.
    cached = get_cached_factory(meta);
    if (cached) {
        co_return *cached;
    }
    auto binary = co_await load();
    if (!binary) {
        // The loader has already reported why. Nothing is cached, so the next
        // caller retries rather than inheriting a failure.
        co_return nullptr;
    }
    auto factory = co_await _underlying->make_factory(
      std::move(meta), std::move(*binary));
    size_t image_bytes = factory->image_bytes();
    auto created = ss::make_shared<cached_factory>(
      ss::make_foreign(std::move(factory)),
      offset,
      &_engine_caches,
      image_bytes);
    if (_cache_epoch == epoch_at_start) {
        _factory_cache.insert_or_assign(offset, created->weak_from_this());
    }
    // Returned either way: the caller asked for a factory and this one is
    // usable. It simply is not shared with whoever comes next, so the next
    // caller compiles under the current grants.
    co_return created;
}

ss::optimized_optional<ss::shared_ptr<factory>>
caching_runtime::find_cached(model::offset offset) {
    auto it = _factory_cache.find(offset);
    if (it == _factory_cache.end() || !it->second) {
        return {};
    }
    return ss::static_pointer_cast<factory>(it->second->shared_from_this());
}

ss::optimized_optional<ss::shared_ptr<factory>>
caching_runtime::get_cached_factory(const model::transform_metadata& meta) {
    auto found = find_cached(meta.source_ptr);
    if (found) {
        // The only reason to take a factory out of the cache is to build an
        // engine from it, so this is the "residency paid off here" signal,
        // read off the path a processor already walks rather than reported
        // separately.
        auto resident = _resident.find(meta.source_ptr);
        if (resident != _resident.end()) {
            resident->second.last_used_at = ss::lowres_clock::now();
        }
    }
    return found;
}

void caching_runtime::invalidate_factory(model::offset offset) {
    ++_cache_epoch;
    _factory_cache.erase(offset);
    // A pin is a strong reference, so leaving it in place would keep the
    // module compiled under the old grants alive - and, worse, make the next
    // residency sweep see this offset as already warm and never recompile it.
    _resident.erase(offset);
}

ss::future<bool> caching_runtime::pin_factory(
  model::transform_metadata meta, binary_loader_fn load) {
    model::offset offset = meta.source_ptr;
    if (_resident.contains(offset)) {
        co_return true;
    }
    if (_resident.size() >= _max_resident_factories && !evict_unused()) {
        ++_resident_admission_failures;
        vlog(
          wasm_log.debug,
          "not keeping the wasm module at offset {} resident: {} of {} slots "
          "are in use and a transform is using all of them",
          offset,
          _resident.size(),
          _max_resident_factories);
        co_return false;
    }
    // Take the slot before suspending, so that admission is decided exactly
    // once per pin no matter what else runs while this one compiles.
    _resident.emplace(
      offset, resident_entry{.pinned_at = ss::lowres_clock::now()});
    auto epoch_at_start = _cache_epoch;
    auto factory = co_await get_or_create_factory(
      std::move(meta), std::move(load));
    auto reservation = _resident.find(offset);
    if (reservation == _resident.end()) {
        // unpin_factory or invalidate_factory ran while this was compiling.
        co_return false;
    }
    if (reservation->second.strong) {
        // Withdrawn and then pinned again while this was compiling, and that
        // later pin has already landed. Leave its result in place.
        co_return true;
    }
    if (!factory || _cache_epoch != epoch_at_start) {
        // Either the binary could not be loaded - the loader has already said
        // why - or a capability change landed while this was compiling, in
        // which case the module was linked under grants that have since been
        // revoked and get_or_create_factory declined to cache it for that
        // same reason. Give the slot back either way; the next sweep pins
        // whatever gets compiled under the current allowlist.
        _resident.erase(reservation);
        co_return false;
    }
    reservation->second.image_bytes = factory->image_bytes();
    reservation->second.strong = std::move(factory);
    vlog(
      wasm_log.debug,
      "keeping the wasm module at offset {} resident ({} bytes of executable "
      "memory)",
      offset,
      reservation->second.image_bytes);
    co_return true;
}

void caching_runtime::unpin_factory(model::offset offset) {
    _resident.erase(offset);
}

bool caching_runtime::is_resident(model::offset offset) const {
    return _resident.contains(offset);
}

std::vector<model::offset> caching_runtime::resident_factories() const {
    std::vector<model::offset> offsets;
    offsets.reserve(_resident.size());
    for (const auto& entry : _resident) {
        offsets.push_back(entry.first);
    }
    return offsets;
}

void caching_runtime::set_max_resident_factories(size_t max) {
    _max_resident_factories = max;
    while (_resident.size() > max) {
        if (evict_unused()) {
            continue;
        }
        // Unlike admission, refusing is not available here: the operator has
        // asked for a smaller footprint, so something in use has to go. Give
        // up whichever module has gone longest without being asked for.
        _resident.erase(
          std::min_element(
            _resident.begin(),
            _resident.end(),
            [](const auto& lhs, const auto& rhs) {
                return lhs.second.last_used_at < rhs.second.last_used_at;
            }));
    }
}

caching_runtime::residency_stats caching_runtime::residency() const {
    residency_stats stats{
      .count = _resident.size(),
      .admission_failures = _resident_admission_failures,
    };
    for (const auto& entry : _resident) {
        stats.image_bytes += entry.second.image_bytes;
    }
    return stats;
}

bool caching_runtime::evict_unused() {
    auto victim = _resident.end();
    for (auto it = _resident.begin(); it != _resident.end(); ++it) {
        if (!it->second.strong) {
            // Still being compiled, so dropping it would free nothing and
            // throw away work that is already underway.
            continue;
        }
        if (it->second.last_used_at != ss::lowres_clock::time_point{}) {
            continue;
        }
        if (
          victim == _resident.end()
          || it->second.pinned_at < victim->second.pinned_at) {
            victim = it;
        }
    }
    if (victim == _resident.end()) {
        return false;
    }
    vlog(
      wasm_log.debug,
      "dropping the resident wasm module at offset {}: no transform has "
      "asked for it since it was kept",
      victim->first);
    _resident.erase(victim);
    return true;
}

void caching_runtime::register_metrics() {
    namespace sm = ss::metrics;
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("wasm_binary"),
      {
        sm::make_gauge(
          "resident_factories",
          [this] { return _resident.size(); },
          sm::description(
            "Number of compiled WebAssembly modules being kept "
            "in memory so that a transform starting later does "
            "not have to compile them again"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "resident_memory_usage",
          [this] { return residency().image_bytes; },
          sm::description(
            "The amount of executable memory held by WebAssembly modules "
            "that are being kept in memory. Divided by the number of "
            "resident modules, this is what sizing the resident module limit "
            "costs for these binaries"))
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "resident_admission_failures",
          [this] { return _resident_admission_failures; },
          sm::description(
            "Number of times a WebAssembly module could not be kept in "
            "memory because the resident module limit was reached and every "
            "resident module was in use"))
          .aggregate({sm::shard_label}),
      });
}

ss::future<int64_t> caching_runtime::do_gc() {
    auto fut = co_await ss::coroutine::as_future(
      ss::when_all_succeed(gc_factories(), gc_engines()));
    _gc_timer.rearm(ss::lowres_clock::now() + _gc_interval);
    if (fut.failed()) {
        auto ex = fut.get_exception();
        vlog(wasm_log.warn, "wasm caching runtime gc failed: {}", ex);
        co_return -1;
    }
    co_return std::apply(std::plus<>(), fut.get());
}

ss::future<int64_t> caching_runtime::gc_factories() {
    return gc_btree_map(&_factory_cache);
}

ss::future<int64_t> caching_runtime::gc_engines() {
    return _engine_caches.map_reduce0(
      &engine_cache::gc, int64_t(0), std::plus<>());
}

ss::future<> caching_runtime::validate(model::wasm_binary_iobuf buf) {
    return _underlying->validate(std::move(buf));
}

} // namespace wasm
