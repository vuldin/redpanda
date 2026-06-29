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

#include "cluster_link/schema_registry_sync/scope.h"

namespace cluster_link::schema_registry_sync {

ss::noncopyable_function<bool(const ppsr::context_subject&)> make_in_scope(
  chunked_hash_set<ppsr::context> contexts,
  chunked_hash_set<ppsr::context_subject> subjects) {
    const bool unfiltered = contexts.empty() && subjects.empty();
    return [unfiltered,
            contexts = std::move(contexts),
            subjects = std::move(subjects)](const ppsr::context_subject& sub) {
        return unfiltered || contexts.contains(sub.ctx)
               || subjects.contains(sub);
    };
}

} // namespace cluster_link::schema_registry_sync
