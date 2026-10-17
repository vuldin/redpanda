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

#include "bytes/iobuf.h"
#include "transform/state_envelope.h"

#include <gtest/gtest.h>

#include <string>

namespace transform {
namespace {

iobuf text(std::string_view s) {
    iobuf b;
    b.append(s.data(), s.size());
    return b;
}

TEST(StateEnvelope, RoundTripsTheOffsetAndTheState) {
    for (auto o : {int64_t{0}, int64_t{1}, int64_t{123456789012}, int64_t{-1}}) {
        auto u = unwrap_state(wrap_state(kafka::offset(o), text("guest state")));
        ASSERT_TRUE(u.input_offset.has_value()) << o;
        EXPECT_EQ(*u.input_offset, kafka::offset(o));
        EXPECT_EQ(u.state, text("guest state"));
    }
}

// An empty region is still a state: its envelope is the header alone.
TEST(StateEnvelope, AnEmptyStateRoundTrips) {
    auto u = unwrap_state(wrap_state(kafka::offset(7), iobuf{}));
    ASSERT_TRUE(u.input_offset.has_value());
    EXPECT_EQ(*u.input_offset, kafka::offset(7));
    EXPECT_EQ(u.state.size_bytes(), 0u);
}

// A checkpoint persisted before the envelope existed comes back whole, with
// no offset, so the restart falls back to the committed offsets.
TEST(StateEnvelope, ACheckpointFromBeforeTheEnvelopePassesThrough) {
    for (auto s : {std::string("a guest region of some size, no magic"),
                   std::string("short"),
                   std::string("")}) {
        auto u = unwrap_state(text(s));
        EXPECT_FALSE(u.input_offset.has_value()) << s;
        EXPECT_EQ(u.state, text(s));
    }
}

} // namespace
} // namespace transform
