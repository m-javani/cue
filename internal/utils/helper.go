// Copyright 2026 M. Javani
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

package utils

import (
	"encoding/binary"
	"os"
	"sync/atomic"

	"github.com/zeebo/blake3"
)

// stringToUint64 converts a string to a uint64 hash using blake3
// This is a high-performance, deterministic hash function
func StringToUint64(s string) uint64 {
	hash := blake3.Sum256([]byte(s))
	return binary.LittleEndian.Uint64(hash[:8])
}

func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()

	return d.Sync()
}

func SafeLoadAtomicString(v *atomic.Value) string {
	if val := v.Load(); val != nil {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}
