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
#include "bytes/iobuf_parser.h"
#include "model/record.h"
#include "serde/rw/rw.h"
#include "transform/stm/state_chunks.h"

#include <gtest/gtest.h>

#include <string>

namespace transform {
namespace {

std::string str(const iobuf& b) {
    iobuf_const_parser p(b);
    return p.read_string(b.size_bytes());
}

state_chunk_info at(uint64_t gen, uint32_t i, uint32_t n) {
    return {.generation = gen, .index = i, .count = n};
}

const model::transform_id tid{7};

} // namespace

TEST(StateChunks, SplitCoversTheStateInOrder) {
    auto parts = split_state(iobuf::from("abcdefghij"), 4);
    ASSERT_EQ(parts.size(), 3);
    EXPECT_EQ(str(parts[0]), "abcd");
    EXPECT_EQ(str(parts[1]), "efgh");
    EXPECT_EQ(str(parts[2]), "ij");
    EXPECT_EQ(split_state(iobuf{}, 4).size(), 1);
}

TEST(StateChunks, OnlyACompleteGenerationIsReturned) {
    state_chunk_assembler a;
    EXPECT_FALSE(a.add(tid, at(1, 0, 3), iobuf::from("ab")));
    EXPECT_FALSE(a.add(tid, at(1, 1, 3), iobuf::from("cd")));
    auto whole = a.add(tid, at(1, 2, 3), iobuf::from("e"));
    ASSERT_TRUE(whole);
    EXPECT_EQ(str(*whole), "abcde");
    EXPECT_EQ(a.assembling(), 0);
}

TEST(StateChunks, ReplayStartingMidCheckpointIsIgnored) {
    // A replica that installed a raft snapshot and replays from an offset
    // inside a checkpoint sees its later chunks without the first.
    state_chunk_assembler a;
    EXPECT_FALSE(a.add(tid, at(1, 1, 3), iobuf::from("cd")));
    EXPECT_FALSE(a.add(tid, at(1, 2, 3), iobuf::from("e")));
    EXPECT_EQ(a.assembling(), 0);
    // ...and the next whole checkpoint assembles normally.
    EXPECT_FALSE(a.add(tid, at(2, 0, 2), iobuf::from("x")));
    auto whole = a.add(tid, at(2, 1, 2), iobuf::from("y"));
    ASSERT_TRUE(whole);
    EXPECT_EQ(str(*whole), "xy");
}

TEST(StateChunks, ANewGenerationAbandonsAPartialOne) {
    // The previous leader died between chunks; its successor starts over.
    state_chunk_assembler a;
    EXPECT_FALSE(a.add(tid, at(1, 0, 3), iobuf::from("old")));
    EXPECT_FALSE(a.add(tid, at(9, 0, 2), iobuf::from("ne")));
    // A stray chunk of the abandoned generation must not splice in.
    EXPECT_FALSE(a.add(tid, at(1, 1, 3), iobuf::from("??")));
    EXPECT_EQ(a.assembling(), 0);
    EXPECT_FALSE(a.add(tid, at(9, 0, 2), iobuf::from("ne")));
    auto whole = a.add(tid, at(9, 1, 2), iobuf::from("w"));
    ASSERT_TRUE(whole);
    EXPECT_EQ(str(*whole), "new");
}

TEST(StateChunks, TransformsAssembleIndependently) {
    state_chunk_assembler a;
    const model::transform_id other{8};
    EXPECT_FALSE(a.add(tid, at(1, 0, 2), iobuf::from("a")));
    EXPECT_FALSE(a.add(other, at(5, 0, 2), iobuf::from("p")));
    auto mine = a.add(tid, at(1, 1, 2), iobuf::from("b"));
    auto theirs = a.add(other, at(5, 1, 2), iobuf::from("q"));
    ASSERT_TRUE(mine && theirs);
    EXPECT_EQ(str(*mine), "ab");
    EXPECT_EQ(str(*theirs), "pq");
}

TEST(StateChunks, AWholeRecordSupersedesPartialChunks) {
    state_chunk_assembler a;
    EXPECT_FALSE(a.add(tid, at(1, 0, 2), iobuf::from("a")));
    a.forget(tid);
    EXPECT_FALSE(a.add(tid, at(1, 1, 2), iobuf::from("b")));
    EXPECT_EQ(a.assembling(), 0);
}

TEST(StateChunks, MalformedInfoIsDropped) {
    state_chunk_assembler a;
    EXPECT_FALSE(a.add(tid, at(1, 0, 0), iobuf::from("a")));
    EXPECT_FALSE(a.add(tid, at(1, 3, 2), iobuf::from("a")));
    EXPECT_EQ(a.assembling(), 0);
}

TEST(StateChunks, HeaderRoundTripsAndWholeRecordsHaveNone) {
    chunked_vector<model::record_header> hdrs;
    auto k = iobuf::from(state_chunk_header_key);
    auto v = serde::to_iobuf(at(42, 3, 5));
    auto ksz = int32_t(k.size_bytes());
    auto vsz = int32_t(v.size_bytes());
    hdrs.emplace_back(ksz, std::move(k), vsz, std::move(v));
    model::record chunked(
      {}, 0, 0, iobuf::from("key"), iobuf::from("val"), std::move(hdrs));
    auto info = state_chunk_of(chunked);
    ASSERT_TRUE(info);
    EXPECT_EQ(*info, at(42, 3, 5));

    model::record whole({}, 0, 0, iobuf::from("key"), iobuf::from("val"), {});
    EXPECT_FALSE(state_chunk_of(whole));
}

} // namespace transform
