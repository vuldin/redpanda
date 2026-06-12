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

#include "base/seastarx.h"
#include "net/types.h"

#include <seastar/core/future.hh>
#include <seastar/net/api.hh>
#include <seastar/util/log.hh>

namespace net::detail {

/// Establishes a TCP connection to \p address, aborting the attempt
/// (and shutting down the socket) once \p timeout passes. Failures are
/// reported with the address included in the error message.
ss::future<ss::connected_socket> dial_single(
  const ss::socket_address& address,
  clock_type::time_point timeout,
  seastar::logger* log);

} // namespace net::detail
