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
#include "model/record.h"
#include "model/transform.h"
#include "serde/envelope.h"
#include "serde/rw/rw.h"

#include <absl/container/flat_hash_map.h>

#include <optional>
#include <string_view>
#include <vector>

namespace transform {

/**
 * Identifies one record of a guest-state checkpoint that was split into
 * chunks (data_transforms_state_checkpoint_chunk_bytes > 0). Carried as a
 * record header rather than in the key or value, so an unchunked checkpoint
 * is byte-for-byte the record it always was and older logs read unchanged.
 *
 * Why split at all: a checkpoint replicates through the input partition's own
 * raft group, and one multi-megabyte entry puts every produce to the
 * partition behind it until it reaches the followers. Measured 2026-10-16 at
 * book depth 10,000 (~2 MB): the slowest half-second intervals recurred
 * exactly every checkpoint, on the produce leg, whether or not the processor
 * waited for the checkpoint. In chunks, produces interleave between them and
 * each waits for at most one chunk.
 */
struct state_chunk_info
  : serde::
      envelope<state_chunk_info, serde::version<0>, serde::compat_version<0>> {
    // Distinguishes one checkpoint's chunks from another's, so a reader can
    // tell a continuation from the start of a different checkpoint.
    uint64_t generation{0};
    uint32_t index{0};
    uint32_t count{0};

    auto serde_fields() { return std::tie(generation, index, count); }
    bool operator==(const state_chunk_info&) const = default;
};

inline constexpr std::string_view state_chunk_header_key = "rp.state_chunk";

/// The chunk header of `r`, or nullopt for a whole (unchunked) record.
inline std::optional<state_chunk_info>
state_chunk_of(const model::record& r) {
    for (const auto& h : r.headers()) {
        if (h.key() == state_chunk_header_key) {
            return serde::from_iobuf<state_chunk_info>(h.value().copy());
        }
    }
    return std::nullopt;
}

/// `state` in pieces of at most `chunk_bytes`, in order. Never empty.
inline std::vector<iobuf> split_state(iobuf state, size_t chunk_bytes) {
    std::vector<iobuf> out;
    iobuf_parser p(std::move(state));
    while (p.bytes_left() > 0) {
        out.push_back(p.share(std::min(chunk_bytes, p.bytes_left())));
    }
    if (out.empty()) {
        out.emplace_back();
    }
    return out;
}

/**
 * Reassembles chunked checkpoints, per transform.
 *
 * One writer replicates a checkpoint's chunks in order while holding the
 * stm's write mutex, so on every replica they apply contiguously and in
 * order. Anything that does not continue the generation being assembled -
 * replay that begins mid-checkpoint after a raft snapshot install, or a
 * leader that died between chunks - drops the partial assembly. Only a
 * complete generation is ever returned, so a partial checkpoint is never what
 * gets restored: the reader keeps the previous complete one.
 */
class state_chunk_assembler {
public:
    /// The whole state if `info` completes its generation, else nullopt.
    std::optional<iobuf>
    add(model::transform_id id, const state_chunk_info& info, iobuf data) {
        if (info.count == 0 || info.index >= info.count) {
            _pending.erase(id);
            return std::nullopt;
        }
        if (info.index == 0) {
            _pending.insert_or_assign(
              id, partial{.generation = info.generation, .next = 0});
        }
        auto it = _pending.find(id);
        if (
          it == _pending.end() || it->second.generation != info.generation
          || it->second.next != info.index) {
            _pending.erase(id);
            return std::nullopt;
        }
        it->second.data.append(std::move(data));
        if (++it->second.next < info.count) {
            return std::nullopt;
        }
        auto whole = std::move(it->second.data);
        _pending.erase(it);
        return whole;
    }

    /// A whole record or tombstone for `id` supersedes any partial chunks.
    void forget(model::transform_id id) { _pending.erase(id); }
    void clear() { _pending.clear(); }
    /// How many transforms have a checkpoint part-way assembled.
    size_t assembling() const { return _pending.size(); }

private:
    struct partial {
        uint64_t generation{0};
        uint32_t next{0};
        iobuf data;
    };
    absl::flat_hash_map<model::transform_id, partial> _pending;
};

} // namespace transform
