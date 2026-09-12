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

#include "model/fundamental.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/transform.h"
#include "ssx/future-util.h"
#include "test_utils/random_bytes.h"
#include "wasm/cache.h"
#include "wasm/engine.h"
#include "wasm/wasi_logger.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

namespace wasm {

namespace {
// NOLINTNEXTLINE(*-avoid-non-const-global-variables,cert-err58-cpp)
static ss::logger test_logger("wasm_cache_test_logger");

struct state {
    std::atomic_int factories = 0;
    std::atomic_int engines = 0;
    std::atomic_int running_engines = 0;
    std::atomic_int engine_restarts = 0;
    // What the next factory created will report as its compiled size. Fixed
    // at construction, the way a real module's image size is fixed by its
    // compile.
    std::atomic_size_t next_factory_image_bytes = 0;

    std::atomic_bool engine_transform_should_throw = false;
};

class fake_logger : public logger {
public:
    fake_logger() = default;
    void log(ss::log_level, std::string_view) noexcept override {}
};

class fake_engine : public engine {
public:
    explicit fake_engine(state* state)
      : _state(state) {
        ++_state->engines;
    }
    fake_engine(const fake_engine&) = delete;
    fake_engine(fake_engine&&) = delete;
    fake_engine& operator=(const fake_engine&) = delete;
    fake_engine& operator=(fake_engine&&) = delete;
    ~fake_engine() override { --_state->engines; }

    ss::future<> start() override {
        ++_state->running_engines;
        if (_has_been_stopped) {
            ++_state->engine_restarts;
        }
        co_return;
    }
    ss::future<> stop() override {
        --_state->running_engines;
        _has_been_stopped = true;
        co_return;
    }

    ss::future<> transform(
      model::record_batch, transform_probe*, transform_callback) override {
        if (_state->engine_transform_should_throw) {
            throw std::runtime_error("test error");
        }
        co_return;
    }

private:
    bool _has_been_stopped = false;
    state* _state;
};

class fake_factory : public factory {
public:
    explicit fake_factory(state* state)
      : _state(state)
      , _image_bytes(state->next_factory_image_bytes) {
        ++_state->factories;
    }
    fake_factory(const fake_factory&) = delete;
    fake_factory(fake_factory&&) = delete;
    fake_factory& operator=(const fake_factory&) = delete;
    fake_factory& operator=(fake_factory&&) = delete;
    ~fake_factory() noexcept override { --_state->factories; }

    ss::future<ss::shared_ptr<engine>>
    make_engine(model::ntp, std::unique_ptr<wasm::logger>) override {
        co_return ss::make_shared<fake_engine>(_state);
    }

    size_t image_bytes() const override { return _image_bytes; }

private:
    state* _state;
    size_t _image_bytes;
};

class fake_runtime : public runtime {
public:
    ss::future<> start(runtime::config) override { co_return; }
    ss::future<> stop() override { co_return; }

    ss::future<ss::shared_ptr<factory>>
    make_factory(model::transform_metadata, model::wasm_binary_iobuf) override {
        co_return ss::make_shared<fake_factory>(&_state);
    }

    state* get_state() { return &_state; }

    ss::future<> validate(model::wasm_binary_iobuf) override { co_return; }

private:
    state _state;
};

} // namespace

class WasmCacheTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        vassert(
          ss::this_smp_shard_count() > 1, "This test expects multiple shards");
    }

    void SetUp() override {
        auto fr = std::make_unique<fake_runtime>();
        _fake_runtime = fr.get();
        // Effectively disable the gc interval
        _caching_runtime = std::make_unique<caching_runtime>(
          std::move(fr), /*gc_interval=*/std::chrono::hours(1));
        _caching_runtime->start({}).get();
        _stopped = false;
    }

    void TearDown() override {
        stop_runtime();
        _fake_runtime = nullptr;
        _caching_runtime = nullptr;
    }

    // Idempotent, so a test can assert on what stopping releases and still
    // let TearDown run.
    void stop_runtime() {
        if (!_stopped) {
            _caching_runtime->stop().get();
            _stopped = true;
        }
    }

    model::transform_metadata random_metadata() {
        _offset = model::next_offset(_offset);
        return {
          .name = tests::random_named_string<model::transform_name>(),
          .input_topic = model::random_topic_namespace(),
          .output_topics = {model::random_topic_namespace()},
          .environment = {},
          .source_ptr = _offset,
        };
    }

    ss::shared_ptr<factory> make_factory(model::transform_metadata metadata) {
        return make_factory_async(std::move(metadata)).get();
    }
    ss::future<ss::shared_ptr<factory>>
    make_factory_async(model::transform_metadata metadata) {
        return _caching_runtime->make_factory(
          std::move(metadata),
          model::wasm_binary_iobuf(
            std::make_unique<iobuf>(_wasm_module.copy())));
    }

    template<typename Func>
    void invoke_on_all(Func&& func) {
        ss::smp::invoke_on_all([func = std::forward<Func>(func)]() mutable {
            return ss::async(std::forward<Func>(func));
        }).get();
    }

    // Counts how many times the loader actually ran, which is the point of
    // get_or_create_factory: the fetch must happen once per compile, not once
    // per caller. It is also how the residency tests below tell a refusal
    // that cost nothing from one that fetched a binary first.
    caching_runtime::binary_loader_fn counting_loader(int* loads) {
        return
          [this,
           loads]() -> ss::future<std::optional<model::wasm_binary_iobuf>> {
              ++*loads;
              // Yield, so a concurrent caller has a chance to observe the
              // in-progress creation rather than racing past it.
              co_await ss::yield();
              co_return model::wasm_binary_iobuf(
                std::make_unique<iobuf>(_wasm_module.copy()));
          };
    }
    static caching_runtime::binary_loader_fn failing_loader() {
        return []() -> ss::future<std::optional<model::wasm_binary_iobuf>> {
            co_return std::nullopt;
        };
    }

    ss::future<ss::shared_ptr<factory>>
    get_or_create_async(model::transform_metadata metadata, int* loads) {
        return _caching_runtime->get_or_create_factory(
          std::move(metadata), counting_loader(loads));
    }
    ss::future<ss::shared_ptr<factory>>
    get_or_create_failing_async(model::transform_metadata metadata) {
        return _caching_runtime->get_or_create_factory(
          std::move(metadata), failing_loader());
    }
    bool is_creating() const { return _caching_runtime->is_creating_factory(); }
    void invalidate(model::offset o) {
        _caching_runtime->invalidate_factory(o);
    }

    ss::future<bool> pin_async(model::transform_metadata metadata, int* loads) {
        return _caching_runtime->pin_factory(
          std::move(metadata), counting_loader(loads));
    }
    bool pin(model::transform_metadata metadata, int* loads) {
        return pin_async(std::move(metadata), loads).get();
    }
    bool pin_failing(model::transform_metadata metadata) {
        return _caching_runtime
          ->pin_factory(std::move(metadata), failing_loader())
          .get();
    }
    void unpin(model::offset o) { _caching_runtime->unpin_factory(o); }
    bool is_resident(model::offset o) const {
        return _caching_runtime->is_resident(o);
    }
    std::vector<model::offset> resident() const {
        return _caching_runtime->resident_factories();
    }
    void set_max_resident(size_t max) {
        _caching_runtime->set_max_resident_factories(max);
    }
    caching_runtime::residency_stats residency() const {
        return _caching_runtime->residency();
    }

    int64_t gc() { return _caching_runtime->do_gc().get(); }
    auto* state() { return _fake_runtime->get_state(); }

    model::record_batch random_batch() const {
        return model::test::make_random_batch(model::test::record_batch_spec{});
    }

private:
    iobuf _wasm_module = tests::random_iobuf();
    model::offset _offset = model::offset(0);
    fake_runtime* _fake_runtime;
    std::unique_ptr<caching_runtime> _caching_runtime;
    bool _stopped = false;
};

void PrintTo(const ss::shared_ptr<factory>& f, std::ostream* os) {
    *os << "factory{" << f.get() << "}";
}

void PrintTo(const ss::shared_ptr<engine>& f, std::ostream* os) {
    *os << "engine{" << f.get() << "}";
}

TEST_F(WasmCacheTest, CachesFactories) {
    auto meta = random_metadata();
    auto factory_one = make_factory_async(meta);
    auto factory_two = make_factory_async(meta);
    EXPECT_EQ(factory_one.get(), factory_two.get());
    EXPECT_EQ(state()->factories, 1);
}

TEST_F(WasmCacheTest, CachesEngines) {
    auto meta = random_metadata();
    auto ntp = model::random_ntp();
    auto factory = ss::make_foreign(make_factory(meta));
    static thread_local ss::shared_ptr<engine> live_engine;
    invoke_on_all([&factory, &ntp] {
        auto engine_one = factory->make_engine(
          ntp, std::make_unique<fake_logger>());
        auto engine_two = factory->make_engine(
          ntp, std::make_unique<fake_logger>());
        auto engine = engine_one.get();
        EXPECT_EQ(engine, engine_two.get());
        live_engine = engine;
    });
    EXPECT_EQ(state()->engines, ss::this_smp_shard_count());

    // This engine doesn't actually create new instances under the hood.
    auto engine
      = factory->make_engine(ntp, std::make_unique<fake_logger>()).get();
    EXPECT_EQ(state()->engines, ss::this_smp_shard_count());
    engine = nullptr;
    EXPECT_EQ(state()->engines, ss::this_smp_shard_count());

    invoke_on_all([] { live_engine = nullptr; });
    EXPECT_EQ(state()->engines, 0);
}

TEST_F(WasmCacheTest, CanMultiplexEngines) {
    auto meta = random_metadata();
    auto ntp = model::random_ntp();
    auto factory = ss::make_foreign(make_factory(meta));
    auto engine_one
      = factory->make_engine(ntp, std::make_unique<fake_logger>()).get();
    auto engine_two
      = factory->make_engine(ntp, std::make_unique<fake_logger>()).get();
    auto engine_three
      = factory->make_engine(ntp, std::make_unique<fake_logger>()).get();
    EXPECT_EQ(state()->engines, 1);
    ss::when_all_succeed(
      [&engine_two] { return engine_two->start(); },
      [&engine_three] { return engine_three->start(); })
      .get();
    EXPECT_EQ(state()->running_engines, 1);
    engine_one->start().get();
    EXPECT_EQ(state()->running_engines, 1);
    engine_one->stop().get();
    EXPECT_EQ(state()->running_engines, 1);
    ss::when_all_succeed(
      [&engine_two] { return engine_two->stop(); },
      [&engine_three] { return engine_three->stop(); })
      .get();
    EXPECT_EQ(state()->running_engines, 0);
}

TEST_F(WasmCacheTest, CanMultiplexTransforms) {
    auto meta = random_metadata();
    auto ntp = model::random_ntp();
    auto factory = ss::make_foreign(make_factory(meta));
    auto engine_one
      = factory->make_engine(ntp, std::make_unique<fake_logger>()).get();
    auto engine_two
      = factory->make_engine(ntp, std::make_unique<fake_logger>()).get();
    engine_one->start().get();
    engine_two->start().get();
    state()->engine_transform_should_throw = true;
    EXPECT_THROW(
      engine_one
        ->transform(
          random_batch(),
          nullptr,
          [](auto, auto, auto) { return ssx::now(write_success::yes); })
        .get(),
      std::runtime_error);
    EXPECT_EQ(state()->engine_restarts, 1);
    state()->engine_transform_should_throw = false;
    EXPECT_NO_THROW(
      engine_two
        ->transform(
          random_batch(),
          nullptr,
          [](auto, auto, auto) { return ssx::now(write_success::yes); })
        .get());
    EXPECT_EQ(state()->engine_restarts, 1);
    engine_one->stop().get();
    engine_two->stop().get();
    EXPECT_EQ(state()->running_engines, 0);
}

TEST_F(WasmCacheTest, GC) {
    auto meta = random_metadata();
    auto ntp = model::random_ntp();
    auto factory = ss::make_foreign(make_factory(meta));
    // Create an engine and destroy it
    invoke_on_all([&factory, &ntp] {
        factory->make_engine(ntp, std::make_unique<fake_logger>()).get();
    });
    EXPECT_EQ(state()->engines, 0);
    // We should GC each engine for each core
    EXPECT_EQ(gc(), ss::this_smp_shard_count());
    factory = nullptr;
    // Now we should GC the factory
    EXPECT_EQ(gc(), 1);
}

TEST_F(WasmCacheTest, FactoryReplacementBeforeGC) {
    auto meta = random_metadata();
    auto factory = make_factory_async(meta).get();
    EXPECT_EQ(state()->factories, 1);
    factory = nullptr;
    EXPECT_EQ(state()->factories, 0);
    // Catches a bug when we were asserting incorrectly because a factory was
    // replaced in the cache instead of being inserted.
    factory = make_factory_async(meta).get();
    EXPECT_EQ(state()->factories, 1);
}

// The engine cache is keyed on (transform offset, ntp), not just transform
// offset: two partitions of the same transform led on this shard must not
// share an engine (and so must not share linear memory, or serialize on each
// other's transform calls).
TEST_F(WasmCacheTest, DifferentPartitionsGetDifferentEngines) {
    auto meta = random_metadata();
    auto factory = ss::make_foreign(make_factory(meta));
    auto engine_one = factory
                        ->make_engine(
                          model::random_ntp(), std::make_unique<fake_logger>())
                        .get();
    auto engine_two = factory
                        ->make_engine(
                          model::random_ntp(), std::make_unique<fake_logger>())
                        .get();
    EXPECT_NE(engine_one, engine_two);
    EXPECT_EQ(state()->engines, 2);
}

TEST_F(WasmCacheTest, EngineReplacementBeforeGC) {
    auto meta = random_metadata();
    auto ntp = model::random_ntp();
    auto factory = ss::make_foreign(make_factory(meta));
    auto engine
      = factory->make_engine(ntp, std::make_unique<fake_logger>()).get();
    EXPECT_EQ(state()->engines, 1);
    engine = nullptr;
    EXPECT_EQ(state()->engines, 0);
    // Catches a bug when we were asserting incorrectly because an engine was
    // replaced in the cache instead of being inserted.
    engine = factory->make_engine(ntp, std::make_unique<fake_logger>()).get();
    EXPECT_EQ(state()->engines, 1);
}

TEST_F(WasmCacheTest, FetchesTheBinaryOncePerCompileNotOncePerCaller) {
    // The case this exists for: after a leadership move, every partition of a
    // transform on this broker asks for the same factory at once. The fetch is
    // an RPC with a 3s timeout, so doing it per caller and discarding all but
    // one is a real share of a cold start, not a rounding error.
    auto meta = random_metadata();
    int loads = 0;
    auto a = get_or_create_async(meta, &loads);
    auto b = get_or_create_async(meta, &loads);
    auto c = get_or_create_async(meta, &loads);
    auto fa = std::move(a).get();
    auto fb = std::move(b).get();
    auto fc = std::move(c).get();

    EXPECT_EQ(loads, 1) << "the binary was fetched more than once";
    EXPECT_EQ(state()->factories, 1);
    // And all three callers got the same factory, not one real and two
    // throwaways.
    EXPECT_EQ(fa.get(), fb.get());
    EXPECT_EQ(fb.get(), fc.get());
}

TEST_F(WasmCacheTest, ACachedFactoryIsReturnedWithoutFetchingAtAll) {
    auto meta = random_metadata();
    int loads = 0;
    auto first = get_or_create_async(meta, &loads).get();
    ASSERT_EQ(loads, 1);

    auto second = get_or_create_async(meta, &loads).get();
    EXPECT_EQ(loads, 1) << "a cache hit still fetched the binary";
    EXPECT_EQ(first.get(), second.get());
}

TEST_F(WasmCacheTest, AFailedFetchCachesNothingSoTheNextCallerRetries) {
    // A fetch can fail for reasons that do not recur - the binary topic's
    // leader moving, an RPC timeout. Caching the failure would strand the
    // transform until something else evicted it.
    auto meta = random_metadata();
    auto failed = get_or_create_failing_async(meta).get();
    EXPECT_FALSE(failed);
    EXPECT_EQ(state()->factories, 0);

    int loads = 0;
    auto retried = get_or_create_async(meta, &loads).get();
    EXPECT_TRUE(retried);
    EXPECT_EQ(loads, 1) << "the retry did not attempt a fetch";
}

TEST_F(WasmCacheTest, IsCreatingFactoryCoversTheFetchNotJustTheCompile) {
    // Speculative work checks this before queueing behind a compile that a
    // running transform is blocked on. If it only covered the compile, the
    // fetch window would look idle.
    EXPECT_FALSE(is_creating());
    auto meta = random_metadata();
    int loads = 0;
    auto pending = get_or_create_async(meta, &loads);
    // The loader yields, so creation is in flight here.
    EXPECT_TRUE(is_creating()) << "a fetch in flight did not register";
    std::move(pending).get();
    EXPECT_FALSE(is_creating());
}

TEST_F(WasmCacheTest, InvalidateForcesTheNextCallerToRecompile) {
    // A factory's capability grants are baked in when it is compiled, so
    // revoking a grant does nothing to an already-compiled module. Dropping
    // the cache entry is what makes the next compile observe the revocation.
    auto meta = random_metadata();
    int loads = 0;
    auto first = get_or_create_async(meta, &loads).get();
    ASSERT_EQ(loads, 1);
    ASSERT_EQ(state()->factories, 1);

    invalidate(meta.source_ptr);

    auto second = get_or_create_async(meta, &loads).get();
    EXPECT_EQ(loads, 2) << "the next caller reused the invalidated factory";
    EXPECT_NE(first.get(), second.get()) << "same factory handed back";
}

TEST_F(WasmCacheTest, AnEngineAlreadyRunningSurvivesInvalidation) {
    // Deliberate non-behaviour: a running engine holds its own strong
    // reference and keeps the grants it was linked with. Tearing it down here
    // would turn a capability change into an unannounced data-plane
    // interruption. What must recompile is a RESTART, per the test above.
    auto meta = random_metadata();
    int loads = 0;
    auto factory = get_or_create_async(meta, &loads).get();
    ASSERT_TRUE(factory);

    invalidate(meta.source_ptr);

    EXPECT_EQ(state()->factories, 1);
    EXPECT_TRUE(factory);
}

TEST_F(WasmCacheTest, InvalidatingAnUncachedOffsetIsHarmless) {
    // The caller is a config watch that does not know which offsets this
    // broker has compiled, so this has to be safe rather than checked.
    invalidate(model::offset(12345));
    EXPECT_EQ(state()->factories, 0);
}

TEST_F(WasmCacheTest, ACompileThatBeganBeforeAnInvalidateIsNotCached) {
    // The race the epoch guard exists for, and the reason the epoch is
    // captured at entry rather than before the compile: this invalidation
    // lands while the FETCH is still in flight, which is the longer of the
    // two windows. Caching the result would immediately undo the
    // invalidation and hand the revoked module to everyone who came later.
    auto meta = random_metadata();
    int loads = 0;
    auto pending = get_or_create_async(meta, &loads);
    ASSERT_TRUE(is_creating());
    invalidate(meta.source_ptr);
    auto factory = std::move(pending).get();

    // The caller still gets a usable factory - it asked for one.
    EXPECT_TRUE(factory);
    // But it was not shared, so the next caller compiles under the new
    // grants rather than inheriting this one.
    auto after = get_or_create_async(meta, &loads).get();
    EXPECT_EQ(loads, 2) << "the pre-invalidation compile was cached anyway";
    EXPECT_NE(factory.get(), after.get());
}

TEST_F(WasmCacheTest, AResidentModuleSurvivesItsLastProcessor) {
    // The exact inverse of FactoryReplacementBeforeGC, which asserts that
    // today a module dies with the last thing using it - before any GC runs,
    // because every cache entry is a weak reference. That is what makes the
    // next start, typically on the far side of a leadership move, pay for the
    // fetch and the compile again.
    set_max_resident(1);
    auto meta = random_metadata();
    int loads = 0;
    ASSERT_TRUE(pin(meta, &loads));
    ASSERT_EQ(loads, 1);

    // A transform takes it, runs, and goes away.
    auto factory = get_or_create_async(meta, &loads).get();
    ASSERT_TRUE(factory);
    ASSERT_EQ(loads, 1);
    factory = nullptr;

    EXPECT_EQ(state()->factories, 1) << "the module died with its processor";
    EXPECT_EQ(gc(), 0) << "gc reaped a module that is being kept resident";

    auto next = get_or_create_async(meta, &loads).get();
    EXPECT_TRUE(next);
    EXPECT_EQ(loads, 1) << "a resident module was fetched and compiled again";
}

TEST_F(WasmCacheTest, ResidencyIsOffUntilItIsGivenABudget) {
    // Also the clearest statement of where admission happens: a refusal must
    // not have fetched anything. That ordering is what forces the bound to
    // count modules rather than bytes - a byte bound could only be applied
    // after the compile it is meant to prevent.
    auto meta = random_metadata();
    int loads = 0;
    EXPECT_FALSE(pin(meta, &loads));
    EXPECT_EQ(loads, 0) << "a refused pin fetched the binary anyway";
    EXPECT_FALSE(is_resident(meta.source_ptr));
    EXPECT_EQ(residency().count, 0u);
    EXPECT_EQ(residency().admission_failures, 1u);
}

TEST_F(WasmCacheTest, PinningWhatIsAlreadyResidentCostsNothing) {
    // The caller reconciles towards a desired set, so it asks on every sweep.
    set_max_resident(1);
    auto meta = random_metadata();
    int loads = 0;
    ASSERT_TRUE(pin(meta, &loads));
    ASSERT_EQ(loads, 1);

    EXPECT_TRUE(pin(meta, &loads));
    EXPECT_EQ(loads, 1) << "a resweep refetched a module it already had";
    EXPECT_EQ(residency().count, 1u);
}

TEST_F(WasmCacheTest, AModuleNoTransformAskedForIsDroppedBeforeRefusing) {
    set_max_resident(1);
    auto never_asked_for = random_metadata();
    auto newcomer = random_metadata();
    int loads = 0;
    ASSERT_TRUE(pin(never_asked_for, &loads));

    EXPECT_TRUE(pin(newcomer, &loads));
    EXPECT_FALSE(is_resident(never_asked_for.source_ptr));
    EXPECT_TRUE(is_resident(newcomer.source_ptr));
    EXPECT_EQ(residency().count, 1u);
    EXPECT_EQ(residency().admission_failures, 0u);
}

TEST_F(WasmCacheTest, TheBoundRefusesRatherThanDropAModuleInUse) {
    // With N+1 transforms competing for N slots, dropping the one in use
    // means it recompiles the next time its partition moves AND the newcomer
    // that displaced it never gets used either - a loop of cold starts on the
    // one thread that compiles. Refusing is strictly better than that.
    set_max_resident(1);
    auto in_use = random_metadata();
    auto newcomer = random_metadata();
    int loads = 0;
    ASSERT_TRUE(pin(in_use, &loads));
    // Asking the cache for it is the "residency paid off" signal.
    ASSERT_TRUE(get_or_create_async(in_use, &loads).get());
    ASSERT_EQ(loads, 1);

    EXPECT_FALSE(pin(newcomer, &loads));
    EXPECT_EQ(loads, 1) << "a refused pin fetched the binary anyway";
    EXPECT_TRUE(is_resident(in_use.source_ptr));
    EXPECT_FALSE(is_resident(newcomer.source_ptr));
    EXPECT_EQ(residency().admission_failures, 1u);
}

TEST_F(WasmCacheTest, ConcurrentPinsCannotOvershootTheBound) {
    // The slot is taken when admission is decided, before the loader runs.
    // Deciding and then inserting after a suspension would make the bound
    // advisory: two pins that both passed the check would both land.
    set_max_resident(1);
    auto first_meta = random_metadata();
    auto second_meta = random_metadata();
    int loads = 0;
    auto first = pin_async(first_meta, &loads);
    auto second = pin_async(second_meta, &loads);
    EXPECT_TRUE(std::move(first).get());
    EXPECT_FALSE(std::move(second).get());
    EXPECT_EQ(loads, 1) << "the refused pin fetched a binary anyway";
    EXPECT_EQ(residency().count, 1u);
}

TEST_F(WasmCacheTest, APinWhoseFetchFailsGivesTheSlotBack) {
    set_max_resident(1);
    ASSERT_FALSE(pin_failing(random_metadata()));
    EXPECT_EQ(residency().count, 0u);
    // A refused fetch is not an admission failure, and the slot has to be
    // genuinely free rather than charged to a module that does not exist
    // here.
    EXPECT_EQ(residency().admission_failures, 0u);
    int loads = 0;
    EXPECT_TRUE(pin(random_metadata(), &loads));
}

TEST_F(WasmCacheTest, InvalidatingAModuleAlsoStopsKeepingItResident) {
    // The regression that matters most: residency must not be able to revert
    // the revocation guarantee. A pin is a strong reference, so a pin left
    // behind would keep the module linked under the old grants alive and make
    // the next sweep see this offset as already warm.
    set_max_resident(1);
    auto meta = random_metadata();
    int loads = 0;
    ASSERT_TRUE(pin(meta, &loads));
    ASSERT_TRUE(is_resident(meta.source_ptr));
    ASSERT_EQ(state()->factories, 1);

    invalidate(meta.source_ptr);

    EXPECT_FALSE(is_resident(meta.source_ptr));
    EXPECT_EQ(state()->factories, 0)
      << "the module with the revoked grants is still in memory";
    auto after = get_or_create_async(meta, &loads).get();
    EXPECT_EQ(loads, 2) << "the next start reused the revoked module";
}

TEST_F(WasmCacheTest, APinInterruptedByAnInvalidationIsNotKept) {
    // Same reasoning as ACompileThatBeganBeforeAnInvalidateIsNotCached, one
    // layer up: this compile was linked under grants that were revoked while
    // it ran, so keeping it resident would hold the revoked module for as
    // long as the transform exists.
    set_max_resident(1);
    auto meta = random_metadata();
    int loads = 0;
    auto pending = pin_async(meta, &loads);
    ASSERT_TRUE(is_creating());

    invalidate(meta.source_ptr);

    EXPECT_FALSE(std::move(pending).get());
    EXPECT_FALSE(is_resident(meta.source_ptr));
    EXPECT_EQ(state()->factories, 0);
}

TEST_F(WasmCacheTest, ResidencyReportsTheExecutableMemoryItHolds) {
    // The knob is a count, because a module's size is only knowable after the
    // compile. This gauge is how an operator turns that count into a memory
    // figure for their own binaries instead of guessing.
    set_max_resident(2);
    int loads = 0;
    state()->next_factory_image_bytes = 4096;
    ASSERT_TRUE(pin(random_metadata(), &loads));
    EXPECT_EQ(residency().image_bytes, 4096u);

    state()->next_factory_image_bytes = 1024;
    ASSERT_TRUE(pin(random_metadata(), &loads));
    EXPECT_EQ(residency().count, 2u);
    EXPECT_EQ(residency().image_bytes, 4096u + 1024u);
}

TEST_F(WasmCacheTest, ResidentFactoriesListsThePinnedOffsets) {
    set_max_resident(2);
    auto first_meta = random_metadata();
    auto second_meta = random_metadata();
    int loads = 0;
    ASSERT_TRUE(pin(first_meta, &loads));
    ASSERT_TRUE(pin(second_meta, &loads));
    EXPECT_EQ(
      resident(),
      (std::vector<model::offset>{
        first_meta.source_ptr, second_meta.source_ptr}));

    unpin(first_meta.source_ptr);
    EXPECT_EQ(resident(), (std::vector<model::offset>{second_meta.source_ptr}));
    EXPECT_EQ(state()->factories, 1);
}

TEST_F(WasmCacheTest, LoweringTheBoundReleasesModulesImmediately) {
    // An operator turning residency down, or off, has to get the memory back
    // now - not whenever every resident module happens to be invalidated.
    set_max_resident(2);
    auto first_meta = random_metadata();
    auto second_meta = random_metadata();
    int loads = 0;
    ASSERT_TRUE(pin(first_meta, &loads));
    ASSERT_TRUE(pin(second_meta, &loads));
    // Both in use, so neither is the free choice that admission prefers.
    ASSERT_TRUE(get_or_create_async(first_meta, &loads).get());
    ASSERT_TRUE(get_or_create_async(second_meta, &loads).get());
    ASSERT_EQ(state()->factories, 2);

    set_max_resident(1);
    EXPECT_EQ(residency().count, 1u);
    EXPECT_EQ(state()->factories, 1);

    set_max_resident(0);
    EXPECT_EQ(residency().count, 0u);
    EXPECT_EQ(state()->factories, 0)
      << "turning residency off kept the memory anyway";
}

TEST_F(WasmCacheTest, StoppingReleasesResidentModules) {
    // A resident module is a cached_factory, which holds a raw pointer to the
    // runtime's per-shard engine caches. A pin outliving the runtime would be
    // holding a dangling pointer, so the pins have to go before those caches
    // are stopped.
    set_max_resident(1);
    int loads = 0;
    ASSERT_TRUE(pin(random_metadata(), &loads));
    ASSERT_EQ(state()->factories, 1);

    stop_runtime();

    EXPECT_EQ(state()->factories, 0);
}

} // namespace wasm
