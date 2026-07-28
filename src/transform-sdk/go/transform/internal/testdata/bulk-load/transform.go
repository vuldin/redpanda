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

// Not part of the transform SDK - see network-echo/transform.go's comment.

//go:wasmimport redpanda_wasm_shared_memory register_region
func registerRegion(ptr unsafe.Pointer, length uint32) int32

//go:wasmimport redpanda_wasm_network bulk_load
func bulkLoad(targetIndex uint32, requestPtr unsafe.Pointer, requestLen uint32) int32

var sharedBuf [256]byte

var request = []byte("give me the snapshot")

func main() {
	if rc := registerRegion(unsafe.Pointer(&sharedBuf[0]), uint32(len(sharedBuf))); rc != 0 {
		panic("failed to register shared memory region")
	}
	// One guest ABI call kicks off the whole dial+send+drain-the-full-
	// response round trip; the host delivers the result directly into
	// sharedBuf above, not back through this call.
	if rc := bulkLoad(0, unsafe.Pointer(&request[0]), uint32(len(request))); rc < 0 {
		panic("bulk_load failed")
	}
	transform.OnRecordWritten(echoSharedMemory)
}

// echoSharedMemory ignores the input record and instead emits whatever
// bulk_load delivered into sharedBuf, trimmed at the first zero byte.
func echoSharedMemory(_ transform.WriteEvent, w transform.RecordWriter) error {
	n := 0
	for n < len(sharedBuf) && sharedBuf[n] != 0 {
		n++
	}
	value := make([]byte, n)
	copy(value, sharedBuf[:n])
	return w.Write(transform.Record{Value: value})
}
