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
#include "transform_processor.h"

#include "bytes/bytes.h"
#include "logger.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "ssx/abort_source.h"
#include "ssx/future-util.h"
#include "wasm/engine.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/variant_utils.hh>

#include <boost/range/irange.hpp>

#include <algorithm>
#include <optional>

namespace transform {

namespace {

// Upper bound on how long run_consumer_loop waits for wait_for_offset to
// notice new data before retrying on its own. This is a safety net, not the
// primary mechanism: the common case resolves as soon as the source's
// underlying offset_monitor is notified, which is typically sub-millisecond.
// Kept at the same order of magnitude as the old fixed poll interval so a
// missed or delayed notification degrades to roughly today's behavior rather
// than stalling indefinitely.
constexpr auto fallback_wait_interval = std::chrono::seconds(1);

// Minimum time between guest-state checkpoints (see
// processor::maybe_checkpoint_state). A checkpoint replicates through this
// partition's own raft group, so pacing it well below batch-processing
// frequency keeps it a coarse, occasional durability improvement rather
// than a per-record cost on the hot path this whole roadmap exists to keep
// fast.
constexpr auto checkpoint_interval = std::chrono::seconds(5);

class queue_output_consumer {
    static constexpr size_t buffer_chunk_size = 8;

public:
    queue_output_consumer(
      transfer_queue<model::record_batch, buffer_chunk_size>* output,
      ss::abort_source* as,
      probe* probe)
      : _output(output)
      , _as(as)
      , _probe(probe) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        // This is a "safe" cast as all our offsets come from the translating
        // reader, but we don't have a seperate type for kafka::record_batch vs
        // model::record_batch.
        _last_offset = model::offset_cast(b.last_offset());
        _probe->increment_read_bytes(b.size_bytes());
        _probe->record_input_delay(b.header().max_timestamp);
        co_await _output->push(std::move(b), _as);
        co_return ss::stop_iteration::no;
    }
    std::optional<kafka::offset> end_of_stream() const { return _last_offset; }

private:
    std::optional<kafka::offset> _last_offset;
    transfer_queue<model::record_batch, buffer_chunk_size>* _output;
    ss::abort_source* _as;
    probe* _probe;
};

struct drain_result {
    ss::chunked_fifo<model::record_batch> batches;
    kafka::offset latest_offset;
};

/**
 * A specific exception to throw on processor::stop so that we're not accidently
 * swallowing unexpected exceptions.
 */
class processor_shutdown_exception : public std::exception {
    const char* what() const noexcept override {
        return "processor shutting down";
    }
};

} // namespace

processor::processor(
  model::transform_id id,
  model::ntp ntp,
  model::transform_metadata meta,
  ss::shared_ptr<wasm::engine> engine,
  state_callback cb,
  std::unique_ptr<source> source,
  std::vector<std::unique_ptr<sink>> sinks,
  std::unique_ptr<offset_tracker> offset_tracker,
  probe* p,
  memory_limits* mem_limits,
  std::unique_ptr<sink> dead_letter_sink,
  std::unique_ptr<state_store> state_store)
  : _id(id)
  , _ntp(std::move(ntp))
  , _meta(std::move(meta))
  , _engine(std::move(engine))
  , _source(std::move(source))
  , _offset_tracker(std::move(offset_tracker))
  , _state_callback(std::move(cb))
  , _probe(p)
  , _consumer_transform_pipe(&mem_limits->read_buffer_semaphore)
  , _outputs()
  , _dead_letter_sink(std::move(dead_letter_sink))
  , _state_store(std::move(state_store))
  , _task(ss::now())
  , _logger(tlog, ss::format("{}/{}", _meta.name(), _ntp.tp.partition())) {
    const auto& outputs = _meta.output_topics;
    vassert(
      outputs.size() == sinks.size(),
      "expected the same number of output topics and sinks");
    _outputs.reserve(outputs.size());
    _last_reported_lag.reserve(outputs.size());
    for (size_t i : boost::irange(outputs.size())) {
        _outputs.emplace(
          outputs[i].tp,
          output{
            .index = model::output_topic_index(i),
            .queue = transfer_queue<transformed_output>(
              &mem_limits->write_buffer_semaphore),
            .sink = std::move(sinks[i])});
        _last_reported_lag.push_back(0);
    }
    _default_output = &_outputs.at(outputs.front().tp);
    // The processor needs to protect against multiple stops (or calling stop
    // before start), to do that we use the state in _as. Start out with an
    // abort so that we handle being stopped before we're started.
    _as.request_abort_ex(processor_shutdown_exception());
}

ss::future<> processor::start() {
    // Don't allow double starts of this module - we use the abort_requested
    // flag to determine if the module is "running" or not.
    if (!_as.abort_requested()) {
        co_return;
    }
    _as = {};
    co_await _source->start();
    co_await _offset_tracker->start();
    if (_state_store) {
        co_await _state_store->start();
    }
    _task = handle_processor_task(
      _engine->start()
        .then([this] { return restore_guest_state(); })
        .then([this] { return load_latest_committed(); })
        .then([this](
                absl::flat_hash_map<model::output_topic_index, kafka::offset>
                  latest_committed) {
            // We start reading from the minimum process all producers have
            // committed too. This means some producers will get transforms
            // replayed, and in that case they can skip those records until
            // they start seeing new records past their committed offset.
            kafka::offset min = kafka::offset::max();
            for (const auto& [_, offset] : latest_committed) {
                // next_offset is used here because we want to start at the
                // offset **after** the last one we've processed.
                min = std::min(min, kafka::next_offset(offset));
            }
            // Mark that we're running now that the start offset is loaded.
            _state_callback(_id, _ntp, state::running);
            return when_all_shutdown(
              run_consumer_loop(min),
              run_transform_loop(),
              run_all_producers(std::move(latest_committed)));
        }));
}

ss::future<> processor::stop() {
    // We reset the abort source when being started, so protect against double
    // stops by checking if we've already requested being stopped.
    if (_as.abort_requested()) {
        co_return;
    }
    auto ex = std::make_exception_ptr(processor_shutdown_exception());
    _as.request_abort_ex(ex);
    co_await std::exchange(_task, ss::now());
    _consumer_transform_pipe.clear();
    for (auto& [_, output] : _outputs) {
        output.queue.clear();
    }
    co_await _source->stop();
    co_await _offset_tracker->stop();
    if (_state_store) {
        co_await _state_store->stop();
    }
    co_await _engine->stop();
    // reset lag now that we've stopped
    for (const auto& [_, output] : _outputs) {
        report_lag(output.index, 0);
    }
}

ss::future<> processor::restore_guest_state() {
    if (!_state_store) {
        co_return;
    }
    auto state = co_await _state_store->load_latest_state();
    if (!state) {
        // Nothing persisted yet - the expected, correct state for a
        // brand-new deploy, or for a transform that's never opted into
        // checkpointing (never registered a shared-memory region). Not
        // distinguishable from "a snapshot existed and was lost" at this
        // layer alone - that's exactly why require_state_recovery is an
        // explicit, transform-owned deploy-time choice rather than
        // something inferred here.
        co_return;
    }
    if (_engine->write_shared_memory(iobuf_to_bytes(*state))) {
        vlog(
          _logger.info,
          "restored a persisted guest-state snapshot ({} bytes)",
          state->size_bytes());
        co_return;
    }
    // We have a persisted snapshot but couldn't deliver it into the
    // guest's memory - the binary may have lost its shared_memory
    // capability grant since the snapshot was taken, or this instance's
    // guest hasn't registered a region (yet, or at all). Silently
    // continuing would run the guest against wrong or zeroed state while
    // a perfectly good snapshot sits unused - exactly the G12 failure
    // mode this PR exists to eliminate.
    _probe->increment_state_recovery_failure();
    vlog(
      _logger.error,
      "failed to restore a persisted guest-state snapshot ({} bytes) into "
      "this transform's guest memory",
      state->size_bytes());
    if (_meta.state_options.require_state_recovery) {
        throw std::runtime_error(
          "failed to restore required guest state on start");
    }
}

ss::future<> processor::maybe_checkpoint_state() {
    if (!_state_store) {
        co_return;
    }
    auto now = ss::lowres_clock::now();
    // _last_checkpoint_at starts at time_point::min() so the very first
    // successful batch always checkpoints - checked explicitly rather
    // than via `now - _last_checkpoint_at`, which overflows the
    // underlying duration when subtracting from min() and would
    // otherwise wrap around to a value that looks smaller than
    // checkpoint_interval, silently skipping that first checkpoint.
    if (
      _last_checkpoint_at != ss::lowres_clock::time_point::min()
      && now - _last_checkpoint_at < checkpoint_interval) {
        co_return;
    }
    auto state = _engine->read_shared_memory();
    if (!state) {
        // No shared_memory capability grant, or no region registered -
        // this transform hasn't opted into state persistence at all.
        // Still counts as "checked", so we don't retry every batch.
        _last_checkpoint_at = now;
        co_return;
    }
    co_await _state_store->save_state(std::move(*state));
    _last_checkpoint_at = now;
}

ss::future<absl::flat_hash_map<model::output_topic_index, kafka::offset>>
processor::load_latest_committed() {
    auto latest_committed = co_await _offset_tracker->load_committed_offsets();
    auto latest = _source->latest_offset();
    auto start = _source->start_offset();
    std::optional<kafka::offset> initial_offset;
    for (const auto& [_, output] : _outputs) {
        auto it = latest_committed.find(output.index);
        if (it == latest_committed.end()) {
            // If we have never committed, determine where we need to start
            // processing log records, from the end or at a specific timestamp.
            if (!initial_offset) {
                initial_offset = co_await ss::visit(
                  _meta.offset_options.position,
                  [this,
                   &latest](model::transform_offset_options::latest_offset) {
                      vlog(_logger.debug, "starting at latest: {}", latest);
                      return ssx::now(latest);
                  },
                  [this, &latest](model::timestamp ts) {
                      vlog(_logger.debug, "starting at timestamp: {}", ts);
                      // We want to *start at* this timestamp, so record that
                      // we're going to commit progress at the offset before, so
                      // we start inclusive of this offset. If nothing has been
                      // committed since the start timestamp, commit progress at
                      // latest (i.e. start from the end)
                      return _source->offset_at_timestamp(ts, &_as).then(
                        [&latest](std::optional<kafka::offset> o)
                          -> ss::future<kafka::offset> {
                            return ssx::now(
                              o.has_value() ? kafka::prev_offset(o.value())
                                            : latest);
                        });
                  },
                  [this, &latest, &start](model::transform_from_start off) {
                      vlog(_logger.debug, "starting at offset: {}", off);
                      auto actual_offset = std::min(start + off.delta, latest);
                      // We want to *start at* this offset, so record that we're
                      // going to commit progress at the offset before, so we
                      // start inclusive of this offset.
                      return ssx::now(kafka::prev_offset(actual_offset));
                  },
                  [this, &latest, &start](model::transform_from_end off) {
                      vlog(_logger.debug, "starting at offset: {}", off);
                      auto actual_offset = std::max(latest - off.delta, start);
                      return ssx::now(actual_offset);
                  });
                vlog(
                  _logger.debug, "resolved start offset: {}", *initial_offset);
            }
            co_await _offset_tracker->commit_offset(
              output.index, *initial_offset);
            latest_committed[output.index] = *initial_offset;
            continue;
        }
        // The latest record is inclusive of the last record, so we want to
        // start reading from the following record.
        auto last_processed_offset = it->second;
        if (
          latest != kafka::offset::min()
          && last_processed_offset == kafka::offset::min()) {
            // In cases where we committed the start of the log without any
            // records, then the log has added records, we will overflow
            // computing small_offset - min_offset. Instead normalize last
            // processed to -1 so that the computed lag is correct (these ranges
            // are inclusive). For example: latest(1) - last_processed(-1) =
            // lag(2)
            last_processed_offset = kafka::offset(-1);
        }
        report_lag(output.index, latest - last_processed_offset);
    }
    co_return latest_committed;
}

ss::future<> processor::run_consumer_loop(kafka::offset offset) {
    vlog(_logger.debug, "starting at offset {}", offset);
    // Producers do not emit records until their previous committed records have
    // been done being processed. In order to notify them this is the start
    // position, we prime the output queue with the previous offset. This
    // signifies that they have processed everything up to this point.
    //
    // This is important because on restart we start processing from the minimum
    // offset.
    for (auto& [_, output] : _outputs) {
        co_await output.queue.push(
          {progress_marker{.offset = kafka::prev_offset(offset)}}, &_as);
    }
    while (!_as.abort_requested()) {
        auto reader = co_await _source->read_batch(offset, &_as);
        auto last_offset = co_await std::move(reader).consume(
          queue_output_consumer(&_consumer_transform_pipe, &_as, _probe),
          model::no_timeout);
        if (!last_offset) {
            vlog(
              _logger.trace,
              "received no results, waiting for offset {} to become "
              "available",
              offset);
            // Bounded so a missed or delayed notification (e.g. no local
            // Raft group, or a Kafka LSO that lags Raft's visible offset
            // under an open transaction) still self-heals within roughly
            // today's old fixed poll interval, rather than waiting
            // indefinitely.
            co_await _source->wait_for_offset(
              offset,
              model::timeout_clock::now() + fallback_wait_interval,
              &_as);
            continue;
        }
        offset = kafka::next_offset(*last_offset);
        vlog(_logger.trace, "consumed up to offset {}", offset);
    }
}

ss::future<> processor::run_transform_loop() {
    while (!_as.abort_requested()) {
        auto batch = co_await _consumer_transform_pipe.pop_one(&_as);
        if (!batch) {
            continue;
        }
        auto offset = model::offset_cast(batch->last_offset());
        // Captured before the batch is handed to the engine so that the
        // producer can report end-to-end latency against the input record's
        // own append time.
        auto source_timestamp = batch->header().max_timestamp;
        ss::chunked_fifo<model::transformed_data> transformed;
        vlog(_logger.trace, "transforming offset {}", offset);
        // Only copied when dead-lettering is actually configured - the one
        // extra cost of that specific opt-in, not something every transform
        // pays. batch is moved into _engine->transform() below regardless,
        // so this is the only chance to keep the original bytes around for
        // a possible dead-letter write below.
        std::optional<model::record_batch> batch_for_dead_letter;
        if (_dead_letter_sink) {
            batch_for_dead_letter = batch->copy();
        }
        bool gave_up_on_batch = false;
        try {
            co_await _engine->transform(
              std::move(*batch),
              _probe,
              [this](
                std::optional<model::topic_view> topic,
                model::transformed_data data) {
                  if (!topic) {
                      return _default_output->queue
                        .push({std::move(data)}, &_as)
                        .then([] { return wasm::write_success::yes; });
                  }
                  auto it = _outputs.find(topic.value());
                  if (it == _outputs.end()) {
                      return ssx::now(wasm::write_success::no);
                  }
                  return it->second.queue.push({std::move(data)}, &_as)
                    .then([] { return wasm::write_success::yes; });
              });
            _consecutive_transform_failures = 0;
            co_await maybe_checkpoint_state();
        } catch (...) {
            // co_await isn't usable inside a catch handler, so this only
            // decides whether to give up - the actual dead-letter write (if
            // any) happens just below, once we're back in normal flow.
            if (_as.abort_requested()) {
                // Shutting down, not a genuine transform failure - propagate
                // as before so the loop exits cleanly, uncounted.
                throw;
            }
            ++_consecutive_transform_failures;
            auto max_retries = _meta.failure_policy.max_retries;
            if (
              !max_retries || _consecutive_transform_failures <= *max_retries) {
                // Preserves the original behavior exactly: propagates to
                // handle_processor_task, which stops this processor and
                // reports state::errored - transform_manager's existing
                // exponential backoff (and its own increment_failure()
                // call) restarts it from here, unchanged.
                throw;
            }
            vlog(
              _logger.error,
              "giving up on offset {} after {} consecutive failures ({}): "
              "{}",
              offset,
              _consecutive_transform_failures,
              _dead_letter_sink ? "dead-lettering" : "skipping",
              std::current_exception());
            _probe->increment_failure();
            _probe->increment_given_up();
            _consecutive_transform_failures = 0;
            gave_up_on_batch = true;
        }
        if (gave_up_on_batch && _dead_letter_sink) {
            ss::chunked_fifo<model::record_batch> batches;
            batches.push_back(std::move(*batch_for_dead_letter));
            co_await _dead_letter_sink->write(std::move(batches));
        }
        vlog(_logger.trace, "transformed offset {}", offset);
        // Mark all queues as processsed up to this point.
        for (auto& [_, output] : _outputs) {
            co_await output.queue.push(
              {progress_marker{
                .offset = offset, .source_timestamp = source_timestamp}},
              &_as);
        }
    }
}

ss::future<> processor::run_all_producers(
  absl::flat_hash_map<model::output_topic_index, kafka::offset>
    latest_committed) {
    // parallel_for_each captures the first exception but waits for every loop
    // to finish before resolving, and a producer loop only exits when its abort
    // source fires. Drive the loops off a composite source that aborts when
    // either the processor stops (_as) or a producer fails (local_producer_as,
    // our "a producer failed" signal), so the first failure unwinds its
    // siblings promptly instead of leaving them spinning forever.
    ss::abort_source local_producer_as;
    ssx::composite_abort_source producers_as(_as, local_producer_as);

    std::exception_ptr failure;
    co_await ss::parallel_for_each(
      _outputs,
      [this,
       &failure,
       &local_producer_as,
       &producers_as,
       committed = std::move(latest_committed)](auto& entry) {
          output& out = entry.second;
          return run_producer_loop(
                   out.index,
                   &out.queue,
                   out.sink.get(),
                   committed.at(out.index),
                   producers_as.as())
            .handle_exception([&failure,
                               &local_producer_as](std::exception_ptr ep) {
                if (!local_producer_as.abort_requested()) {
                    failure = std::move(ep);
                    local_producer_as.request_abort_ex(
                      std::make_exception_ptr(processor_shutdown_exception()));
                }
            });
      });
    if (failure) {
        std::rethrow_exception(failure);
    }
}

ss::future<> processor::run_producer_loop(
  model::output_topic_index index,
  transfer_queue<transformed_output>* queue,
  sink* sink,
  kafka::offset last_committed,
  ss::abort_source& as) {
    vlog(
      _logger.debug,
      "starting producer {} - last committed: {}",
      index,
      last_committed);
    // It's possible that we have to reprocess some data, in that case we want
    // to suppress records until we've reached the previous offset we've
    // committed.
    bool suppress = true;
    while (!as.abort_requested()) {
        auto popped = co_await queue->pop_all(&as);
        if (popped.empty()) {
            continue;
        }
        kafka::offset latest_offset = last_committed;
        std::optional<model::timestamp> latest_source_timestamp;
        ss::chunked_fifo<model::transformed_data> records;
        for (auto& entry : popped) {
            ss::visit(
              entry.data,
              [&records, &suppress](model::transformed_data& d) {
                  if (suppress) {
                      return;
                  }
                  records.push_back(std::move(d));
              },
              [&latest_offset,
               &latest_source_timestamp,
               &suppress,
               last_committed](const progress_marker& marker) {
                  // Stop supressing new records when we see new records from
                  // the last commit.
                  // This can happen if other sinks are behind this one and we
                  // have to replay history.
                  suppress = last_committed > marker.offset;
                  latest_offset = marker.offset;
                  if (marker.source_timestamp.has_value()) {
                      latest_source_timestamp = marker.source_timestamp;
                  }
              });
        }
        while (!records.empty()) {
            // TODO(rockwood): Lookup the output topic size instead of
            // hardcoding the default value.
            static constexpr size_t default_batch_size_limit = 1_MiB;
            size_t current_size = 0;
            ss::chunked_fifo<model::transformed_data> batch_records;
            while (!records.empty()) {
                auto& next = records.front();
                size_t next_size = current_size
                                   + next.estimated_serialized_size();
                if (
                  !batch_records.empty()
                  && next_size > default_batch_size_limit) {
                    break;
                }
                current_size = next_size;
                batch_records.push_back(std::move(next));
                records.pop_front();
            }
            auto batch = model::transformed_data::make_batch(
              model::timestamp::now(), std::move(batch_records));
            if (_meta.compression_mode != model::compression::none) {
                batch = co_await model::compress_batch(
                  _meta.compression_mode, std::move(batch));
            }
            _probe->increment_write_bytes(index, batch.size_bytes());
            ss::chunked_fifo<model::record_batch> batches;
            batches.push_back(std::move(batch));
            co_await sink->write(std::move(batches));
        }
        if (latest_offset > last_committed) {
            vlog(
              _logger.trace,
              "committing progress {} for output topic {}",
              latest_offset,
              index);
            co_await _offset_tracker->commit_offset(index, latest_offset);
            report_lag(index, _source->latest_offset() - latest_offset);
            last_committed = latest_offset;
            // The output for this input is now written and its progress
            // committed, so this is the point at which the record is fully
            // through the pipeline.
            if (latest_source_timestamp.has_value()) {
                _probe->record_e2e_latency(*latest_source_timestamp);
            }
        }
    }
}

template<typename... T>
ss::future<> processor::when_all_shutdown(T&&... futs) {
    return ss::when_all_succeed(handle_processor_task(std::forward<T>(futs))...)
      .discard_result();
}

ss::future<> processor::handle_processor_task(ss::future<> fut) {
    try {
        co_await std::move(fut);
    } catch (const processor_shutdown_exception& e) {
        // Do nothing, this is an expected error on shutdown
        std::ignore = e;
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error running transform: {}", ex);
        _state_callback(_id, _ntp, state::errored);
    }
}

void processor::report_lag(model::output_topic_index idx, int64_t lag) {
    int64_t delta = lag - _last_reported_lag[idx()];
    _probe->report_lag(idx, delta);
    _last_reported_lag[idx()] = lag;
}

model::transform_id processor::id() const { return _id; }
const model::ntp& processor::ntp() const { return _ntp; }
const model::transform_metadata& processor::meta() const { return _meta; }
bool processor::is_running() const {
    // Only mark this as running if we've called start without calling stop.
    return !_as.abort_requested();
}

int64_t processor::current_lag() const {
    return *std::ranges::max_element(_last_reported_lag);
}

size_t transformed_output::memory_usage() const {
    return sizeof(transformed_output)
           + ss::visit(
             data,
             [](const model::transformed_data& d) {
                 // Account for the size of this struct, but don't double count
                 // sizeof(model::transformed_data), which d.memory_usage()
                 // accounts for.
                 return d.memory_usage() - sizeof(model::transformed_data);
             },
             [](const progress_marker&) { return size_t(0); });
}

} // namespace transform
