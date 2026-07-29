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
	"strconv"
	"unsafe"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

// Raw imports against the host module, same reasoning as network-echo's own
// - not part of the SDK's public ABI.

//go:wasmimport redpanda_wasm_network connect
func netConnect(targetIndex uint32, outHandle unsafe.Pointer) int32

//go:wasmimport redpanda_wasm_network send
func netSend(handle int32, bufPtr unsafe.Pointer, bufLen uint32) int32

var message = []byte("push payload sent in a tight loop against a peer that never reads")

const numPushes = 10000

var connHandle int32

func main() {
	if rc := netConnect(0, unsafe.Pointer(&connHandle)); rc != 0 {
		panic("connect failed")
	}
	transform.OnRecordWritten(sendManyPushes)
}

// Called once per input record by the C++ test driver - deliberately NOT
// done in main(), unlike network-echo's single send/recv: send() calls made
// here happen inside the same batch the test's transform() call drives, so
// the host's per-batch push drain (which only runs around a batch, not
// around main()) actually gets exercised against them.
func sendManyPushes(_ transform.WriteEvent, w transform.RecordWriter) error {
	successCount := 0
	for i := 0; i < numPushes; i++ {
		if rc := netSend(connHandle, unsafe.Pointer(&message[0]), uint32(len(message))); rc == 0 {
			successCount++
		}
		// Any other return code (e.g. the buffered queue filling up) is
		// an accepted, expected outcome of this fix's design - not
		// asserted against here, since it couples this guest to the
		// host's internal buffer-size constant. The C++ test only checks
		// that at least some pushes succeeded and that this whole loop
		// didn't block on the unresponsive peer.
	}
	return w.Write(transform.Record{Value: []byte(strconv.Itoa(successCount))})
}
