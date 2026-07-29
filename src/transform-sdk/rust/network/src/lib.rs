// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Redpanda Data Transforms Rust outbound networking client.
//!
//! Grants a trusted transform real outbound TCP connections to a
//! cluster-admin-configured allowlist of targets - the `network`
//! capability and `allowed_targets` in `config::wasm_trusted_modules`.
//! There is no way to reach anywhere else from inside a transform using
//! this client: [`NetworkClient::connect`] takes an index into that
//! allowlist, never a raw address.
//!
//! This client can only ever be used to push data out and read responses
//! back - a transform cannot accept inbound connections or otherwise be
//! called into from outside Redpanda.
//!
//! # Sends are enqueued, not written inline
//!
//! [`Connection::send`] copies its argument into an internal queue and
//! returns immediately; the actual write happens later, off the critical
//! path of whatever transform callback called it. A slow or unreachable
//! peer can never make `send` block - the cost of that is that a send can
//! be silently dropped (surfaced as [`NetworkError::BufferFull`]) if the
//! queue is still full of undelivered pushes from a peer that isn't
//! keeping up. This is deliberate: this client is meant for best-effort,
//! low-latency side-channel use, not as a reliable transport - Redpanda's
//! own durable topics remain the source of truth for anything that can't
//! tolerate an occasional dropped message.

use redpanda_transform_sdk_network_sys::{AbiConnection, AbiNetworkClient};
pub use redpanda_transform_sdk_network_sys::{NetworkError, Result};

/// A client for opening outbound connections from within a data transform.
#[derive(Debug, Default)]
pub struct NetworkClient {
    delegate: AbiNetworkClient,
}

impl NetworkClient {
    /// Creates a new network client. Requires the transform binary to be
    /// granted the `network` capability - see the crate-level docs.
    pub fn new() -> Self {
        Self {
            delegate: AbiNetworkClient::new(),
        }
    }

    /// Connects to `allowed_targets[target_index]`, as configured by the
    /// cluster admin for this transform - there is no way to specify a
    /// raw address here.
    pub fn connect(&self, target_index: u32) -> Result<Connection> {
        Ok(Connection {
            inner: self.delegate.connect(target_index)?,
        })
    }
}

/// An open connection to one of the transform's `allowed_targets`.
/// Automatically closed on drop.
#[derive(Debug)]
pub struct Connection {
    inner: AbiConnection,
}

impl Connection {
    /// Enqueues `buf` to be sent on this connection - see the crate-level
    /// docs for what "enqueued, not written inline" means here.
    pub fn send(&self, buf: &[u8]) -> Result<()> {
        self.inner.send(buf)
    }

    /// Reads up to `buf.len()` bytes into `buf`, returning the number of
    /// bytes actually read - 0 on a graceful remote close, which is not
    /// itself an error.
    pub fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        self.inner.recv(buf)
    }
}
