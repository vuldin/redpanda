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

// registerRegion is not part of the transform SDK - it is a raw import
// against the host module a specifically-trusted wasm binary is granted
// (config::wasm_capability::shared_memory), declared directly here rather
// than in the SDK package, since it isn't part of the SDK's public ABI.
//
//go:wasmimport redpanda_wasm_shared_memory register_region
func registerRegion(ptr unsafe.Pointer, length uint32) int32

// sharedBuf is the region the host writes into. Its address, once this
// binary is running, does not change - Go's wasip1 runtime does not move
// already-allocated globals - so registering it once, in main, is enough.
var sharedBuf [64]byte

func main() {
	if rc := registerRegion(unsafe.Pointer(&sharedBuf[0]), uint32(len(sharedBuf))); rc != 0 {
		panic("failed to register shared memory region")
	}
	transform.OnRecordWritten(echoSharedMemory)
}

// echoSharedMemory ignores the input record entirely and instead emits
// whatever the host has most recently written into sharedBuf, trimmed at
// the first zero byte - this is what the C++ test asserts against, to
// verify a host-initiated write (with no corresponding guest ABI call at
// all) actually lands in memory this guest can read directly.
func echoSharedMemory(_ transform.WriteEvent, w transform.RecordWriter) error {
	n := 0
	for n < len(sharedBuf) && sharedBuf[n] != 0 {
		n++
	}
	value := make([]byte, n)
	copy(value, sharedBuf[:n])
	return w.Write(transform.Record{Value: value})
}
