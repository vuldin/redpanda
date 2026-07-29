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

//! An internal crate providing the ABI contract for Redpanda's Data Transform
//! outbound networking client.
//!
//! If you are looking to use transform networking you probably want crate
//! [redpanda-transform-sdk-network](https://crates.io/crates/redpanda-transform-sdk-network).

#[cfg(target_os = "wasi")]
mod abi;
#[cfg(not(target_os = "wasi"))]
mod stub_abi;
#[cfg(not(target_os = "wasi"))]
use stub_abi as abi;

use std::fmt;

/// Error codes network_module's host ABI can return, mirrored 1:1 - the
/// integer values are only meaningful relative to
/// `src/v/wasm/network_module.cc` (the constants named there), which is
/// the source of truth if these ever need to change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkError {
    InvalidTargetIndex,
    ConnectionLimitExceeded,
    ConnectFailed,
    InvalidHandle,
    IoError,
    ResponseTooLarge,
    TimedOut,
    NoSharedMemoryRegion,
    /// The host's pending-push queue was already full when this send was
    /// attempted, so it was dropped rather than queued - the expected,
    /// accepted outcome of send() being a best-effort side channel under
    /// sustained backpressure, not a bug to work around.
    BufferFull,
    Unknown(i32),
}

impl NetworkError {
    fn from_errno(errno: i32) -> Self {
        match errno {
            -1 => Self::InvalidTargetIndex,
            -2 => Self::ConnectionLimitExceeded,
            -3 => Self::ConnectFailed,
            -4 => Self::InvalidHandle,
            -5 => Self::IoError,
            -6 => Self::ResponseTooLarge,
            -7 => Self::TimedOut,
            -8 => Self::NoSharedMemoryRegion,
            -9 => Self::BufferFull,
            other => Self::Unknown(other),
        }
    }
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTargetIndex => write!(f, "invalid target index"),
            Self::ConnectionLimitExceeded => {
                write!(f, "too many open connections for this transform instance")
            }
            Self::ConnectFailed => write!(f, "failed to connect to target"),
            Self::InvalidHandle => write!(f, "connection handle is closed or unknown"),
            Self::IoError => write!(f, "network I/O error"),
            Self::ResponseTooLarge => write!(f, "response exceeded the bulk_load size limit"),
            Self::TimedOut => write!(f, "operation timed out"),
            Self::NoSharedMemoryRegion => {
                write!(
                    f,
                    "no shared memory region registered to receive the response"
                )
            }
            Self::BufferFull => write!(f, "pending push queue is full - this send was dropped"),
            Self::Unknown(code) => write!(f, "unknown network error (code {code})"),
        }
    }
}

impl std::error::Error for NetworkError {}

pub type Result<T> = std::result::Result<T, NetworkError>;

/// A client for opening outbound connections from within a data transform.
/// Requires the transform binary to be granted the `network` capability in
/// `config::wasm_trusted_modules` - see network_module.h's own doc comment
/// for exactly what that grants and what it doesn't.
#[derive(Debug, Default)]
pub struct AbiNetworkClient {}

impl AbiNetworkClient {
    pub fn new() -> Self {
        unsafe {
            abi::check();
        }
        Self {}
    }

    /// Connects to `allowed_targets[target_index]`, as configured by the
    /// cluster admin for this transform - there is no way to specify a
    /// raw address here, by design.
    pub fn connect(&self, target_index: u32) -> Result<AbiConnection> {
        let mut handle: i32 = 0;
        let errno = unsafe { abi::connect(target_index, &mut handle) };
        if errno != 0 {
            return Err(NetworkError::from_errno(errno));
        }
        Ok(AbiConnection { handle })
    }
}

/// A raw ABI-backed connection handle. Automatically closed on drop.
#[derive(Debug)]
pub struct AbiConnection {
    handle: i32,
}

impl AbiConnection {
    /// Enqueues `buf` to be sent on this connection - the host copies it
    /// into an internal queue and returns immediately; the actual write
    /// happens later, off this call's critical path. See
    /// redpanda-transform-sdk-network's own docs for the full reasoning -
    /// the short version is that this call can never block on a slow or
    /// unreachable peer.
    pub fn send(&self, buf: &[u8]) -> Result<()> {
        let errno = unsafe { abi::send(self.handle, buf.as_ptr(), buf.len() as u32) };
        if errno != 0 {
            return Err(NetworkError::from_errno(errno));
        }
        Ok(())
    }

    /// Reads up to `buf.len()` bytes into `buf`, returning the number of
    /// bytes actually read - 0 on a graceful remote close, which is not
    /// itself an error.
    pub fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        let mut out_len: u32 = 0;
        let errno = unsafe {
            abi::recv(
                self.handle,
                buf.as_mut_ptr(),
                buf.len() as u32,
                &mut out_len,
            )
        };
        if errno != 0 {
            return Err(NetworkError::from_errno(errno));
        }
        Ok(out_len as usize)
    }
}

impl Drop for AbiConnection {
    fn drop(&mut self) {
        // Best-effort, mirroring the host's own close() semantics (a
        // no-op if the handle is already gone) - there's no meaningful
        // way to act on a failure here, and no caller left to report one
        // to.
        unsafe {
            abi::close(self.handle);
        }
    }
}
