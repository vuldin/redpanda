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

#include "transform_module.h"

#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "ffi.h"
#include "logger.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "wasi.h"

#include <seastar/core/condition-variable.hh>

#include <algorithm>
#include <exception>
#include <optional>
#include <utility>

namespace wasm {

namespace {
constexpr int32_t NO_ACTIVE_TRANSFORM = -1;
constexpr int32_t INVALID_BUFFER = -2;
constexpr int32_t INVALID_WRITE = -3;

struct write_options {
    std::optional<model::topic_view> topic;
    // Guest-chosen output partition key - a view into options_buf, so
    // callers must copy it out before that guest memory becomes invalid.
    std::optional<std::string_view> partition_key;

    static std::optional<write_options> parse(ffi::array<uint8_t> buffer) {
        constexpr uint8_t output_topic_key = 0x01;
        constexpr uint8_t partition_key_key = 0x02;

        ffi::reader r(buffer);
        write_options opts;
        while (r.remaining_bytes() > 0) {
            switch (r.read_byte()) {
            case output_topic_key:
                if (opts.topic) {
                    return std::nullopt;
                }
                opts.topic = model::topic_view(r.read_sized_string_view());
                break;
            case partition_key_key:
                if (opts.partition_key) {
                    return std::nullopt;
                }
                opts.partition_key = r.read_sized_string_view();
                break;
            default:
                return std::nullopt;
            }
        }
        return opts;
    }
};

} // namespace

transform_module::transform_module(
  std::vector<model::topic> valid_output_topics)
  : _valid_output_topics(std::move(valid_output_topics)) {}

ss::future<> transform_module::for_each_record_async(
  model::record_batch input, record_callback* cb) {
    vassert(
      input.header().attrs.compression() == model::compression::none,
      "wasm transforms expect uncompressed batches");

    iobuf_const_parser parser(input.data());

    ss::chunked_fifo<record_metadata> records;
    records.reserve(input.record_count());
    size_t max_size = 0;

    while (parser.bytes_left() > 0) {
        auto [record_size, rs_amt] = parser.read_varlong();
        auto attrs = parser.consume_type<model::record_attributes::type>();
        auto [timestamp_delta, td_amt] = parser.read_varlong();
        auto [offset_delta, od_amt] = parser.read_varlong();
        size_t meta_size = sizeof(decltype(attrs)) + td_amt + od_amt;
        size_t payload_size = record_size - meta_size;
        max_size = std::max(payload_size, max_size);
        parser.skip(payload_size);
        model::timestamp ts = input.header().max_timestamp;
        if (
          input.header().attrs.timestamp_type()
          == model::timestamp_type::create_time) {
            ts = model::timestamp(
              input.header().first_timestamp() + timestamp_delta);
        }
        records.push_back({
          .metadata_size = rs_amt + meta_size,
          .payload_size = payload_size,
          .attributes = model::record_attributes(attrs),
          .timestamp = ts,
          .offset = input.base_offset() + offset_delta,
        });
    }

    _call_ctx.emplace(
      batch_transform_context{
        .batch_header = input.header(),
        .batch_data = std::move(input).release_data(),
        .max_input_record_size = max_size,
        .records = std::move(records),
        .callback = cb,
      });

    // Draining is only attempted on the success path: a failure here means
    // the engine is stopping or restarting (transform_module::stop() breaks
    // the condition variables), and anything still buffered is lost exactly
    // like any other in-flight state on error/restart - not a new loss mode
    // introduced by buffering writes.
    try {
        co_await host_wait_for_proccessing();
        co_await drain_pending_writes();
    } catch (...) {
        _call_ctx = std::nullopt;
        throw;
    }
    _call_ctx = std::nullopt;
}

void transform_module::check_abi_version_1() {
    // This function does nothing at runtime, it's only an opportunity for
    // static analysis of the module to determine which ABI version to use.
}

void transform_module::check_abi_version_2() {
    // This function does nothing at runtime, it's only an opportunity for
    // static analysis of the module to determine which ABI version to use.
}

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
ss::future<int32_t> transform_module::read_batch_header(
  int64_t* base_offset,
  int32_t* record_count,
  int32_t* partition_leader_epoch,
  int16_t* attributes,
  int32_t* last_offset_delta,
  int64_t* base_timestamp,
  int64_t* max_timestamp,
  int64_t* producer_id,
  int16_t* producer_epoch,
  int32_t* base_sequence) {
    // NOLINTEND(bugprone-easily-swappable-parameters)

    // If we are processing a batch (this isn't the first time this is called),
    // then we need to notify that we've finished processing this batch.
    if (_call_ctx) {
        _call_ctx->callback->post_record();
    }

    co_await guest_wait_for_batch();

    if (!_call_ctx) {
        co_return NO_ACTIVE_TRANSFORM;
    }
    // The wait above can be arbitrarily long for a caught-up, idle
    // transform (it only resolves once new data actually arrives) -
    // without this, the deadline set by the last pre_record() (or, for
    // the very first batch, VM start) is still whatever it was before
    // the wait began, so an idle gap longer than the configured
    // per-invocation timeout guarantees this batch's first record traps
    // immediately, regardless of how fast it would actually process.
    // Found via a real ~1s tail-latency outlier that traced back to a
    // restart firing within 1ms of VM start, i.e. an idle-then-busy
    // transform poisoning its own first record on wakeup.
    _call_ctx->callback->reset_deadline();
    const model::record_batch_header& header = _call_ctx->batch_header;
    *base_offset = header.base_offset();
    *record_count = header.record_count;
    *partition_leader_epoch = int32_t(header.ctx.term());
    *attributes = header.attrs.value();
    *last_offset_delta = header.last_offset_delta;
    *base_timestamp = header.first_timestamp();
    *max_timestamp = header.max_timestamp();
    *producer_id = header.producer_id;
    *producer_epoch = header.producer_epoch;
    *base_sequence = header.base_sequence;

    co_return _call_ctx->max_input_record_size;
}

int32_t transform_module::read_next_record(
  uint8_t* attributes,
  int64_t* timestamp,
  model::offset* offset,
  ffi::array<uint8_t> buf) {
    if (!_call_ctx || _call_ctx->records.empty()) {
        return NO_ACTIVE_TRANSFORM;
    }

    // Callback that we finished processing the previous record,
    // but don't call this the first record that has been read.
    if (
      _call_ctx->records.size()
      != size_t(_call_ctx->batch_header.record_count)) {
        _call_ctx->callback->post_record();
    }

    auto record = _call_ctx->records.front();
    if (buf.size() < record.payload_size) {
        vlog(
          wasm_log.debug,
          "read_record invalid buffer size: {} < {}",
          buf.size(),
          record.payload_size);
        // Buffer wrong size
        return INVALID_BUFFER;
    }
    _call_ctx->records.pop_front();

    // Pass back the record's metadata
    *attributes = record.attributes.value();
    *timestamp = record.timestamp();
    *offset = record.offset;

    // Drop the metadata we already parsed
    _call_ctx->batch_data.trim_front(record.metadata_size);
    // Copy out the payload
    {
        iobuf_const_parser parser(_call_ctx->batch_data);
        parser.consume_to(record.payload_size, buf.data());
    }
    // Skip over the payload
    _call_ctx->batch_data.trim_front(record.payload_size);

    // Call back so we can refuel.
    _call_ctx->callback->pre_record();

    return int32_t(record.payload_size);
}

int32_t transform_module::write_record(ffi::array<uint8_t> buf) {
    if (!_call_ctx) {
        return NO_ACTIVE_TRANSFORM;
    }
    iobuf b;
    b.append(buf.data(), buf.size());
    auto d = model::transformed_data::create_validated(std::move(b));
    if (!d) {
        return INVALID_BUFFER;
    }
    _call_ctx->pending_writes.push_back(
      pending_write{
        .topic = std::nullopt,
        .partition_key = std::nullopt,
        .data = *std::move(d)});
    return int32_t(buf.size());
}

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
int32_t transform_module::write_record_with_options(
  ffi::array<uint8_t> buf, ffi::array<uint8_t> options_buf) {
    // NOLINTEND(bugprone-easily-swappable-parameters)
    if (!_call_ctx) {
        return NO_ACTIVE_TRANSFORM;
    }
    iobuf b;
    b.append(buf.data(), buf.size());
    auto d = model::transformed_data::create_validated(std::move(b));
    if (!d) {
        return INVALID_BUFFER;
    }
    auto options = write_options::parse(options_buf);
    if (!options) {
        return INVALID_BUFFER;
    }
    if (!is_valid_output_topic(options->topic)) {
        return INVALID_WRITE;
    }
    // Own the topic name and partition key: options_buf is guest memory,
    // only valid for the duration of this call, but pending_writes
    // outlives it.
    std::optional<model::topic> owned_topic;
    if (options->topic) {
        owned_topic = model::topic(*options->topic);
    }
    std::optional<iobuf> owned_partition_key;
    if (options->partition_key) {
        owned_partition_key = iobuf();
        owned_partition_key->append(
          options->partition_key->data(), options->partition_key->size());
    }
    _call_ctx->pending_writes.push_back(
      pending_write{
        .topic = std::move(owned_topic),
        .partition_key = std::move(owned_partition_key),
        .data = *std::move(d)});
    return int32_t(buf.size());
}

bool transform_module::is_valid_output_topic(
  const std::optional<model::topic_view>& topic) const {
    if (!topic) {
        return true;
    }
    return std::ranges::any_of(
      _valid_output_topics, [&topic](const model::topic& valid) {
          return model::topic_view(valid) == *topic;
      });
}

ss::future<> transform_module::drain_pending_writes() {
    if (!_call_ctx) {
        co_return;
    }
    auto pending = std::exchange(_call_ctx->pending_writes, {});
    while (!pending.empty()) {
        auto w = std::move(pending.front());
        pending.pop_front();
        std::optional<model::topic_view> topic_view;
        if (w.topic) {
            topic_view = model::topic_view(*w.topic);
        }
        auto success = co_await _call_ctx->callback->emit(
          topic_view, std::move(w.partition_key), std::move(w.data));
        if (success == write_success::no) {
            // Should be unreachable: write_record_with_options already
            // validated the topic synchronously via is_valid_output_topic.
            vlog(
              wasm_log.error,
              "dropped a buffered transform write to output topic {} that "
              "passed synchronous validation",
              w.topic);
        }
    }
}

void transform_module::start() {
    _guest_cond_var.emplace();
    _host_cond_var.emplace();
}

void transform_module::stop(const std::exception_ptr& ex) {
    if (_guest_cond_var) {
        _guest_cond_var->broken(ex);
    }
    if (_host_cond_var) {
        _host_cond_var->broken(ex);
    }
}

ss::future<> transform_module::host_wait_for_proccessing() {
    _guest_cond_var->signal();
    return _host_cond_var->wait();
}

ss::future<> transform_module::guest_wait_for_batch() {
    _host_cond_var->signal();
    return _guest_cond_var->wait();
}

ss::future<> transform_module::await_ready() { return _host_cond_var->wait(); }
} // namespace wasm
