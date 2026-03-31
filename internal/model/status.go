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

package model

type ClusterNodeStatus uint32

const (
	NodeStatusFollowerActive ClusterNodeStatus = 71
	NodeStatusUnavailable    ClusterNodeStatus = 72
	NodeStatusLeaderActive   ClusterNodeStatus = 73
)

func (s ClusterNodeStatus) ToUin32() uint32 {
	return uint32(s)
}

func (s ClusterNodeStatus) String() string {
	switch s {
	case NodeStatusFollowerActive:
		return "follower"
	case NodeStatusLeaderActive:
		return "leader"
	case NodeStatusUnavailable:
		return "unavailable"
	default:
		return "unknown"
	}
}

func ClusterNodeStatusFromUint32(value uint32) ClusterNodeStatus {
	switch value {
	case 71:
		return NodeStatusFollowerActive
	case 73:
		return NodeStatusLeaderActive
	default:
		return NodeStatusUnavailable
	}
}
