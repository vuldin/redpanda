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

#pragma once

#include "pandaproxy/schema_registry/fwd.h"
#include "relay/fwd.h"
#include "wasm/engine.h"

#include <seastar/core/sharded.hh>

#include <memory>

namespace wasm {

// relay, when non-null, is fanned transform output out to by transform
// processors and delivers relay-pushed data into relay-consumer guests'
// shared memory. Owned by the application, outlives the runtime.
std::unique_ptr<runtime> create_default_runtime(
  pandaproxy::schema_registry::api* schema_reg,
  ss::sharded<relay::service>* relay = nullptr);

} // namespace wasm
