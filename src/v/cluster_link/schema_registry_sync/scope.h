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

#include "cluster_link/model/types.h"
#include "container/chunked_hash_map.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <optional>

namespace cluster_link::schema_registry_sync {

namespace ppsr = pandaproxy::schema_registry;

/// Builds a pure predicate reporting whether a context-qualified subject
/// belongs to the link's scope, following the configured source filter's union
/// semantics:
///   * neither `contexts` nor `subjects` configured: no filter, every node is
///     in scope;
///   * otherwise a node is in scope if its context is one of `contexts`, OR it
///     is one of the individually listed `subjects`.
/// A subject filter therefore widens scope -- it adds individual subjects from
/// contexts the context filter does not select -- rather than narrowing it.
ss::noncopyable_function<bool(const ppsr::context_subject&)> make_in_scope(
  chunked_hash_set<ppsr::context> contexts,
  chunked_hash_set<ppsr::context_subject> subjects);

/// Returns a fault reason if the configuration would write a non-default
/// context to the destination while `qualified_subjects_enabled` is off (the
/// destination can then only represent the default context), else nullopt. The
/// flag is injected to keep the check pure and testable.
std::optional<ss::sstring> check_preconditions(
  const model::schema_registry_sync_config& config,
  const chunked_hash_set<ppsr::context>& in_scope_contexts,
  bool qualified_subjects_enabled);

} // namespace cluster_link::schema_registry_sync
