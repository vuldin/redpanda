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

#include "absl/container/flat_hash_map.h"
#include "base/seastarx.h"
#include "model/fundamental.h"
#include "model/transform.h"
#include "transform/io.h"

#include <seastar/core/future.hh>

namespace transform {

/**
 * A minimal offset_tracker for relay-sourced consumers. Relay offsets are
 * synthetic (assigned per push, no durable log behind them), so there is
 * nothing meaningful to commit to the transform_offsets topic - committing
 * those would just be confusing. This tracks the latest committed offset in
 * memory only, which is enough for the processor's lag reporting; a relay
 * consumer always starts from "now" on (re)start, matching the best-effort
 * relay model. Durable progress + replay come from the output topic, not the
 * relay.
 */
class relay_offset_tracker final : public offset_tracker {
public:
    relay_offset_tracker() = default;

    ss::future<> start() final { return ss::now(); }
    ss::future<> stop() final { return ss::now(); }

    // No durable commits to load - always empty, so the consumer starts from
    // the source's latest offset.
    ss::future<absl::flat_hash_map<model::output_topic_index, kafka::offset>>
    load_committed_offsets() final {
        co_return absl::flat_hash_map<model::output_topic_index, kafka::offset>{};
    }

    ss::future<> commit_offset(model::output_topic_index idx, kafka::offset o) final {
        _committed[idx] = o;
        co_return;
    }

private:
    absl::flat_hash_map<model::output_topic_index, kafka::offset> _committed;
};

} // namespace transform
