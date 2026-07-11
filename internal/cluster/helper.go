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

package cluster

import (
	"encoding/binary"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func encodeBufferedEntry(entry *raftpb.Entry) (BufferedEntry, error) {
	data, err := proto.Marshal(entry)
	if err != nil {
		return BufferedEntry{}, err
	}

	totalSize := uint64(16 + len(data))

	var header [16]byte
	binary.LittleEndian.PutUint64(header[0:8], totalSize)
	binary.LittleEndian.PutUint64(header[8:16], entry.GetIndex())

	buf := make([]byte, 16+len(data))
	copy(buf[:16], header[:])
	copy(buf[16:], data)

	return BufferedEntry{
		Index: entry.GetIndex(),
		Data:  buf,
	}, nil
}
