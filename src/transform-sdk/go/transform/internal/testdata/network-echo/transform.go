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

package main

import (
	"unsafe"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

// These are not part of the transform SDK - raw imports against the host
// module a specifically-trusted wasm binary is granted
// (config::wasm_capability::network), declared directly here rather than
// in the SDK package, since they aren't part of the SDK's public ABI.

//go:wasmimport redpanda_wasm_network connect
func netConnect(targetIndex uint32, outHandle unsafe.Pointer) int32

//go:wasmimport redpanda_wasm_network send
func netSend(handle int32, bufPtr unsafe.Pointer, bufLen uint32) int32

//go:wasmimport redpanda_wasm_network recv
func netRecv(handle int32, bufPtr unsafe.Pointer, bufLen uint32, outLen unsafe.Pointer) int32

var message = []byte("ping from the guest")

// echoedResponse holds what the test server sent back, captured once in
// main() - before any transform() call - and re-emitted on every input
// record, so the C++ test can assert on it without needing the guest to
// re-run the network round trip per record.
var echoedResponse []byte

func main() {
	var handle int32
	if rc := netConnect(0, unsafe.Pointer(&handle)); rc != 0 {
		panic("connect failed")
	}
	if rc := netSend(handle, unsafe.Pointer(&message[0]), uint32(len(message))); rc != 0 {
		panic("send failed")
	}
	buf := make([]byte, 256)
	var n uint32
	if rc := netRecv(handle, unsafe.Pointer(&buf[0]), uint32(len(buf)), unsafe.Pointer(&n)); rc != 0 {
		panic("recv failed")
	}
	echoedResponse = buf[:n]

	transform.OnRecordWritten(echoResponse)
}

func echoResponse(_ transform.WriteEvent, w transform.RecordWriter) error {
	return w.Write(transform.Record{Value: echoedResponse})
}
