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

use redpanda_transform_sdk::*;
use redpanda_transform_sdk_network::NetworkClient;

// This example shows a pass-through transform that also pushes every
// record's value out over a real outbound connection - target index 0
// in this transform's `allowed_targets`, as configured by the cluster
// admin in `wasm_trusted_modules`. The record is still written to the
// output topic unchanged either way: the network push is a best-effort
// side channel, not a replacement for it. A dropped push (the peer
// falling behind, or being unreachable) does not fail the transform.

fn main() {
    let client = NetworkClient::new();
    let conn = client
        .connect(0)
        .expect("failed to connect to allowed_targets[0]");
    on_record_written(move |ev, w| push_and_forward(ev, w, &conn));
}

fn push_and_forward(
    event: WriteEvent,
    writer: &mut RecordWriter,
    conn: &redpanda_transform_sdk_network::Connection,
) -> Result<(), WriteError> {
    if let Some(value) = event.record.value() {
        // Errors here (e.g. NetworkError::BufferFull under sustained
        // backpressure) are deliberately not propagated - see the
        // redpanda-transform-sdk-network crate docs for why this is by
        // design, not something to retry or fail the transform over.
        let _ = conn.send(value);
    }
    writer.write(BorrowedRecord::new(
        event.record.key(),
        event.record.value(),
    ))
}
