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

#include "json/document.h"
#include "model/timestamp.h"
#include "wasm/tests/wasm_fixture.h"

#include <algorithm>

TEST_F(WasmTestFixture, Wasi) {
    load_wasm("wasi.wasm");
    auto batch = make_tiny_batch();
    // Brackets the actual wall-clock call transform() makes into the
    // guest. REALTIME_CLOCK_ID is a genuine broker wall-clock reading - it
    // no longer echoes the record's own timestamp, which a producer fully
    // controls, so this can only assert a real-time bound, not exact
    // equality against batch.header().first_timestamp() the way it used
    // to.
    model::timestamp before = model::timestamp::now();
    auto result = transform(batch);
    model::timestamp after = model::timestamp::now();
    const auto& result_records = result.copy_records();
    ASSERT_EQ(result_records.size(), 1);
    const auto& value = result_records.front().value().linearize_to_string();
    json::Document doc;
    doc.Parse(value);
    std::vector<std::string> program_args;
    auto args = doc["Args"].GetArray();
    for (const auto& arg : args) {
        std::string_view v{arg.GetString(), arg.GetStringLength()};
        program_args.emplace_back(v);
    }
    std::vector<std::string> expected_args{meta().name()};
    ASSERT_EQ(program_args, expected_args);

    std::vector<std::string> environment_variables;
    auto env = doc["Env"].GetArray();
    for (const auto& var : env) {
        std::string_view v{var.GetString(), var.GetStringLength()};
        environment_variables.emplace_back(v);
    }
    // The order here doesn't matter, so sort the values.
    std::ranges::sort(environment_variables);
    std::vector<std::string> expected_env{
      ss::format("REDPANDA_INPUT_TOPIC={}", meta().input_topic.tp()),
      ss::format(
        "REDPANDA_OUTPUT_TOPIC_0={}", meta().output_topics.begin()->tp()),
    };
    ASSERT_EQ(environment_variables, expected_env);

    using namespace std::chrono;
    nanoseconds before_ns = duration_cast<nanoseconds>(
      milliseconds(before.value()));
    nanoseconds after_ns = duration_cast<nanoseconds>(
      milliseconds(after.value()));
    ASSERT_GE(doc["NowNanos"].GetInt64(), before_ns.count());
    ASSERT_LE(doc["NowNanos"].GetInt64(), after_ns.count());

    // The random number computed in wasm is dependent on how go computes
    // it's initial seed for it's random number generator.
    //
    // ASSERT_EQ(doc["RandomNumber"].GetInt(), 240963032);
}
