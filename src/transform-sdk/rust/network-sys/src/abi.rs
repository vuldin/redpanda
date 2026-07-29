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

#[link(wasm_import_module = "redpanda_wasm_network")]
extern "C" {
    #[link_name = "check_abi_version_0"]
    pub(crate) fn check();

    #[link_name = "connect"]
    pub(crate) fn connect(target_index: u32, out_handle: *mut i32) -> i32;

    #[link_name = "send"]
    pub(crate) fn send(handle: i32, buf: *const u8, buf_len: u32) -> i32;

    #[link_name = "recv"]
    pub(crate) fn recv(handle: i32, buf: *mut u8, buf_len: u32, out_len: *mut u32) -> i32;

    #[link_name = "close"]
    pub(crate) fn close(handle: i32) -> i32;
}
