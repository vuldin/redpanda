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

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <optional>

namespace transform {

/**
 * A guest-state checkpoint tagged with the input offset it reflects.
 *
 * The checkpoint is copied between invocations, so the guest's state is
 * exactly "every input record through offset k applied". Persisting k with it
 * is what lets a restart resume the input at k+1 instead of at the
 * independently committed output offsets, which can be seconds away from the
 * state either side: replaying from the state's own offset re-applies exactly
 * the records the state lacks, so a deterministic guest re-emits the outputs
 * it emitted before, identically, and a consumer that acted on one early
 * (precommit relay delivery) sees at most a duplicate, never an output the
 * recovered engine does not produce again.
 *
 * Kept in the processor, around the opaque bytes the state store persists, so
 * the store, its chunking and its on-disk format are unchanged. Layout:
 *
 *   [magic: 8 bytes "RPXFOFS1"][input offset: int64 little endian][state]
 *
 * A checkpoint written before this envelope existed has no magic, and
 * unwrap_state() returns it whole with no offset: the restart then resumes
 * from the committed offsets, as before.
 */
inline constexpr std::array<char, 8> state_envelope_magic = {
  'R', 'P', 'X', 'F', 'O', 'F', 'S', '1'};
inline constexpr size_t state_envelope_header_size
  = state_envelope_magic.size() + sizeof(int64_t);

inline iobuf wrap_state(kafka::offset input_offset, iobuf state) {
    iobuf out;
    out.append(state_envelope_magic.data(), state_envelope_magic.size());
    auto v = static_cast<int64_t>(input_offset());
    std::array<char, sizeof(int64_t)> le{};
    for (size_t i = 0; i < le.size(); ++i) {
        le[i] = static_cast<char>((static_cast<uint64_t>(v) >> (8 * i)) & 0xff);
    }
    out.append(le.data(), le.size());
    out.append(std::move(state));
    return out;
}

struct unwrapped_state {
    // The input offset the state reflects; std::nullopt for a checkpoint
    // written before the envelope existed.
    std::optional<kafka::offset> input_offset;
    iobuf state;
};

inline unwrapped_state unwrap_state(iobuf buf) {
    if (buf.size_bytes() < state_envelope_header_size) {
        return {std::nullopt, std::move(buf)};
    }
    iobuf_const_parser p(buf);
    auto magic = p.read_bytes(state_envelope_magic.size());
    if (
      std::memcmp(
        magic.data(), state_envelope_magic.data(), state_envelope_magic.size())
      != 0) {
        return {std::nullopt, std::move(buf)};
    }
    auto le = p.read_bytes(sizeof(int64_t));
    uint64_t v = 0;
    for (size_t i = 0; i < sizeof(int64_t); ++i) {
        v |= static_cast<uint64_t>(static_cast<uint8_t>(le[i])) << (8 * i);
    }
    buf.trim_front(state_envelope_header_size);
    return {kafka::offset(static_cast<int64_t>(v)), std::move(buf)};
}

} // namespace transform
