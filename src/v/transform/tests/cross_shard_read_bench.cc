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

// josh-432: what it costs a transform to read its input partition from a
// core other than the partition's own. A processor today runs on the shard
// that owns its input partition, so on a single very hot partition the
// transform and the partition's produce/replication work share one core.
// Running the processor elsewhere means every read becomes a cross-shard
// call. This measures that read both ways - on the partition's shard, and
// from the other shard with the batches copied back - with the partition's
// shard idle and kept busy by a synthetic load, and reports latency
// percentiles plus the CPU each read costs the partition's shard.
//
// Not a pass/fail test: it prints a table. Run with
//   bazel test -c opt //src/v/transform/tests:cross_shard_read_bench \
//     --test_output=all

#include "base/vassert.h"
#include "kafka/data/partition_proxy.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "storage/record_batch_builder.h"
#include "test_utils/boost_fixture.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <algorithm>
#include <chrono>
#include <numeric>

using namespace std::chrono_literals;

namespace {

// The gateway demux's input: 650 B events, and ~6 per batch as a linger-0
// producer delivers them at 30k/sec (measured on the cluster, 2026-10-17).
constexpr size_t records_per_batch = 6;
constexpr size_t record_bytes = 650;
constexpr size_t batches = 3000;
constexpr int ops = 4000;

model::record_batch make_batch() {
    storage::record_batch_builder b(
      model::record_batch_type::raft_data, model::offset(0));
    for (size_t i = 0; i < records_per_batch; ++i) {
        auto v = random_generators::gen_alphanum_string(record_bytes);
        iobuf value;
        value.append(v.data(), v.size());
        b.add_raw_kv(iobuf{}, std::move(value));
    }
    return std::move(b).build();
}

// One read as partition_source::read_batch does it: sync the start offset,
// clamp to the last stable offset, read one batch's worth.
ss::future<chunked_vector<model::record_batch>>
read_one(cluster::partition_manager& pm, model::ntp ntp, kafka::offset at) {
    auto p = kafka::make_partition_proxy(ntp, pm);
    vassert(p.has_value(), "no partition {}", ntp);
    auto start = co_await p->sync_effective_start();
    vassert(start.has_value(), "sync_effective_start failed");
    auto lso = p->last_stable_offset();
    vassert(lso.has_value(), "no lso");
    auto max = model::offset_cast(model::prev_offset(lso.value()));
    auto from = std::max(model::offset_cast(start.value()), at);
    auto last = std::min(
      max, kafka::offset(from() + records_per_batch - 1));
    auto translated = co_await p->make_reader(
      kafka::log_reader_config(from, last, 0, 128_KiB, std::nullopt, {}));
    co_return co_await model::consume_reader_to_chunked_vector(
      std::move(translated.reader), model::no_timeout);
}

struct dist {
    double p50, p99, p999, mean;
};

dist summarize(std::vector<int64_t> ns) {
    std::sort(ns.begin(), ns.end());
    auto at = [&](double q) {
        return ns[std::min(ns.size() - 1, size_t(q * double(ns.size())))]
               / 1000.0;
    };
    double mean = double(std::accumulate(ns.begin(), ns.end(), int64_t{0}))
                  / double(ns.size()) / 1000.0;
    return {at(0.5), at(0.99), at(0.999), mean};
}

// Keeps the calling shard busy for `frac` of the time in ~20us slices of
// pure CPU - the shape of a hot partition's produce and replication tasks.
// frac >= 1 keeps the task queue never empty.
struct hog {
    double frac;
    bool stop = false;
    std::vector<ss::future<>> loops;

    static void spin(std::chrono::microseconds d) {
        auto until = std::chrono::steady_clock::now() + d;
        while (std::chrono::steady_clock::now() < until) {
        }
    }

    ss::future<> loop() {
        constexpr auto slice = 20us;
        auto rest = std::chrono::duration_cast<std::chrono::microseconds>(
          slice * ((1.0 - std::min(frac, 1.0)) / std::max(frac, 0.01)));
        while (!stop) {
            spin(slice);
            if (frac >= 1.0) {
                co_await ss::yield();
            } else {
                co_await ss::sleep(rest);
            }
        }
    }

    void start() {
        if (frac <= 0) {
            return;
        }
        // Several loops when saturated, so a waiting task always finds the
        // queue occupied.
        int n = frac >= 1.0 ? 4 : 1;
        for (int i = 0; i < n; ++i) {
            loops.push_back(loop());
        }
    }

    ss::future<> finish() {
        stop = true;
        for (auto& f : loops) {
            co_await std::move(f);
        }
    }
};

} // namespace

struct cross_shard_fixture : redpanda_thread_fixture {
    model::ntp local_ntp;  // a partition on shard 0, where the test runs
    model::ntp remote_ntp; // a partition on shard 1
    ss::shard_id remote_shard = 1;

    cross_shard_fixture() {
        vassert(ss::this_smp_shard_count() >= 2, "needs two shards");
        wait_for_controller_leadership().get();
        model::topic t("xshard");
        add_topic(model::topic_namespace_view(model::kafka_namespace, t), 8)
          .get();
        std::optional<model::ntp> on0, on1;
        for (int i = 0; i < 8; ++i) {
            auto ntp = make_default_ntp(t, model::partition_id(i));
            wait_for_leader(ntp).get();
            auto s = app.shard_table.local().shard_for(ntp);
            if (s == 0 && !on0) {
                on0 = ntp;
            } else if (s == remote_shard && !on1) {
                on1 = ntp;
            }
        }
        vassert(on0 && on1, "no partition on each of shards 0 and 1");
        local_ntp = *on0;
        remote_ntp = *on1;
        fill(local_ntp, 0);
        fill(remote_ntp, remote_shard);
    }

    void fill(model::ntp ntp, ss::shard_id shard) {
        app.partition_manager
          .invoke_on(
            shard,
            [ntp](cluster::partition_manager& pm) -> ss::future<> {
                auto p = pm.get(ntp);
                for (size_t done = 0; done < batches; done += 100) {
                    chunked_vector<model::record_batch> v;
                    for (int i = 0; i < 100; ++i) {
                        v.push_back(make_batch());
                    }
                    co_await p->raft()
                      ->replicate(
                        std::move(v),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .discard_result();
                }
            })
          .get();
    }

    static kafka::offset pick() {
        return kafka::offset(
          int64_t(random_generators::get_int<size_t>(0, batches - 2))
          * int64_t(records_per_batch));
    }

    // A load on the partition's shard for the duration of a measurement,
    // owned there (a hog's coroutines must run and end on that shard).
    uintptr_t start_hog(double load) {
        return ss::smp::submit_to(
                 remote_shard,
                 [load] {
                     auto* p = new hog{.frac = load};
                     p->start();
                     return uintptr_t(p);
                 })
          .get();
    }

    void stop_hog(uintptr_t h) {
        ss::smp::submit_to(remote_shard, [h] {
            auto* p = reinterpret_cast<hog*>(h);
            return p->finish().finally([p] { delete p; });
        }).get();
    }

    // CPU the shard has spent so far.
    std::chrono::nanoseconds busy(ss::shard_id s) {
        return ss::smp::submit_to(
                 s,
                 [] {
                     return std::chrono::duration_cast<
                       std::chrono::nanoseconds>(
                       ss::engine().total_busy_time());
                 })
          .get();
    }
};

FIXTURE_TEST(cross_shard_read_cost, cross_shard_fixture) {
    struct row {
        double load;
        dist hop, local, remote;
        double local_cpu_us, remote_cpu_us, caller_copy_us;
    };
    std::vector<row> rows;

    for (double load : {0.0, 0.5, 0.85, 1.0}) {
        row r{.load = load};

        // An empty cross-shard round trip: the bare cost of crossing.
        {
            uintptr_t h = start_hog(load);
            std::vector<int64_t> ns;
            ns.reserve(ops);
            for (int i = 0; i < ops; ++i) {
                auto t0 = std::chrono::steady_clock::now();
                ss::smp::submit_to(remote_shard, [] {}).get();
                ns.push_back((std::chrono::steady_clock::now() - t0).count());
            }
            stop_hog(h);
            r.hop = summarize(std::move(ns));
        }

        // The read on the partition's own shard, under the same load there:
        // today's placement.
        {
            auto ntp = remote_ntp;
            auto c0 = busy(remote_shard);
            auto res = app.partition_manager
                         .invoke_on(
                           remote_shard,
                           [ntp, load](cluster::partition_manager& pm)
                             -> ss::future<std::vector<int64_t>> {
                               hog h{.frac = load};
                               h.start();
                               std::vector<int64_t> ns;
                               ns.reserve(ops);
                               for (int i = 0; i < ops; ++i) {
                                   auto t0 = std::chrono::steady_clock::now();
                                   auto v = co_await read_one(pm, ntp, pick());
                                   vassert(!v.empty(), "empty read");
                                   ns.push_back(
                                     (std::chrono::steady_clock::now() - t0)
                                       .count());
                               }
                               co_await h.finish();
                               co_return ns;
                           })
                         .get();
            auto c1 = busy(remote_shard);
            r.local = summarize(std::move(res));
            // Only meaningful idle: under load the shard's busy time is mostly
            // the hog's.
            r.local_cpu_us = double((c1 - c0).count()) / ops / 1000.0;
        }

        // The same read issued from shard 0, batches copied back to it.
        {
            auto ntp = remote_ntp;
            uintptr_t h = start_hog(load);
            auto c0 = busy(remote_shard);
            std::vector<int64_t> ns;
            ns.reserve(ops);
            std::chrono::nanoseconds copy_ns{0};
            for (int i = 0; i < ops; ++i) {
                auto at = pick();
                auto t0 = std::chrono::steady_clock::now();
                auto foreign
                  = app.partition_manager
                      .invoke_on(
                        remote_shard,
                        [ntp, at](cluster::partition_manager& pm) {
                            return read_one(pm, ntp, at).then([](auto v) {
                                return ss::make_foreign(
                                  std::make_unique<
                                    chunked_vector<model::record_batch>>(
                                    std::move(v)));
                            });
                        })
                      .get();
                auto t1 = std::chrono::steady_clock::now();
                // The batches must be this shard's memory before the
                // transform may keep them.
                chunked_vector<model::record_batch> mine;
                for (auto& b : *foreign) {
                    mine.push_back(b.copy());
                }
                vassert(!mine.empty(), "empty remote read");
                foreign = {};
                auto t2 = std::chrono::steady_clock::now();
                copy_ns += t2 - t1;
                ns.push_back((t2 - t0).count());
            }
            auto c1 = busy(remote_shard);
            stop_hog(h);
            r.remote = summarize(std::move(ns));
            r.remote_cpu_us = double((c1 - c0).count()) / ops / 1000.0;
            r.caller_copy_us = double(copy_ns.count()) / ops / 1000.0;
        }
        rows.push_back(r);
    }

    fmt::print(
      "\ncross-shard read cost: one {}-record batch of {} B records per "
      "read, {} reads per cell; latency in us\n",
      records_per_batch,
      record_bytes,
      ops);
    fmt::print(
      "{:>10} | {:>22} | {:>22} | {:>22} | {:>10} | {:>10} | {:>8}\n",
      "load on",
      "empty hop p50/p99/p999",
      "local read p50/p99/p999",
      "remote read p50/p99/p999",
      "local cpu",
      "remote cpu",
      "copy");
    fmt::print(
      "{:>10} | {:>22} | {:>22} | {:>22} | {:>10} | {:>10} | {:>8}\n",
      "part shard",
      "",
      "",
      "",
      "us/read",
      "us/read",
      "us/read");
    for (auto& r : rows) {
        auto d = [](const dist& x) {
            return fmt::format("{:.1f}/{:.1f}/{:.1f}", x.p50, x.p99, x.p999);
        };
        fmt::print(
          "{:>9.0f}% | {:>22} | {:>22} | {:>22} | {:>10} | {:>10} | {:>8.2f}\n",
          r.load * 100,
          d(r.hop),
          d(r.local),
          d(r.remote),
          r.load == 0 ? fmt::format("{:.2f}", r.local_cpu_us) : "-",
          r.load == 0 ? fmt::format("{:.2f}", r.remote_cpu_us) : "-",
          r.caller_copy_us);
    }
}

namespace {
// Arrival latencies recorded on the receiving shard by the push test.
thread_local std::vector<int64_t> push_arrivals;
} // namespace

// The alternative to pulling with a read: the partition's shard hands each
// batch it appends straight to the transform's shard. Here the partition's
// shard takes a share of the batch (a reference, no byte copy) and sends it;
// the transform's shard copies the bytes into its own memory and drops the
// share, which goes back to the partition's shard to be released. That is
// the whole cost to the hot shard: the share, the send and the release.
FIXTURE_TEST(cross_shard_push_cost, cross_shard_fixture) {
    struct row {
        double load;
        dist arrive;
        double part_cpu_us;
    };
    std::vector<row> rows;

    for (double load : {0.0, 0.5, 0.85, 1.0}) {
        row r{.load = load};
        ss::smp::submit_to(0, [] { push_arrivals.clear(); }).get();
        auto c0 = busy(remote_shard);
        ss::smp::submit_to(
          remote_shard,
          [load]() -> ss::future<> {
              hog h{.frac = load};
              h.start();
              // What the produce path has just appended.
              std::vector<model::record_batch> appended;
              for (int i = 0; i < 64; ++i) {
                  appended.push_back(make_batch());
              }
              for (int i = 0; i < ops; ++i) {
                  auto t0 = std::chrono::steady_clock::now();
                  auto shared = ss::make_foreign(
                    std::make_unique<model::record_batch>(
                      appended[i % appended.size()].share()));
                  co_await ss::smp::submit_to(
                    0, [t0, shared = std::move(shared)]() mutable {
                        auto mine = shared->copy();
                        vassert(mine.record_count() > 0, "empty push");
                        push_arrivals.push_back(
                          (std::chrono::steady_clock::now() - t0).count());
                        shared = {};
                    });
              }
              co_await h.finish();
          })
          .get();
        auto c1 = busy(remote_shard);
        r.part_cpu_us = double((c1 - c0).count()) / ops / 1000.0;
        r.arrive = summarize(
          ss::smp::submit_to(0, [] { return push_arrivals; }).get());
        rows.push_back(r);
    }

    fmt::print(
      "\ncross-shard push cost: one {}-record batch of {} B records per "
      "push, {} pushes per cell; latency in us\n",
      records_per_batch,
      record_bytes,
      ops);
    fmt::print(
      "{:>10} | {:>26} | {:>14}\n",
      "load on",
      "append -> arrival p50/p99/p999",
      "part shard cpu");
    fmt::print("{:>10} | {:>26} | {:>14}\n", "part shard", "", "us/push");
    for (auto& r : rows) {
        fmt::print(
          "{:>9.0f}% | {:>26} | {:>14}\n",
          r.load * 100,
          fmt::format(
            "{:.1f}/{:.1f}/{:.1f}", r.arrive.p50, r.arrive.p99, r.arrive.p999),
          r.load == 0 ? fmt::format("{:.2f}", r.part_cpu_us) : "-");
    }
}
