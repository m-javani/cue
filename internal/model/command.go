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

import (
	"errors"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type CommandType uint8

const (
	CmdUpdatePeersList CommandType = iota
	CmdAddNode
	CmdRemoveNode
	CmdTransferLeader
	CmdAddJob
	CmdDone
	CmdDrop
)

func (t CommandType) String() string {
	switch t {
	case CmdUpdatePeersList:
		return "CmdUpdatePeersList"
	case CmdAddNode:
		return "CmdAddNode"
	case CmdRemoveNode:
		return "CmdRemoveNode"
	case CmdTransferLeader:
		return "CmdTransferLeader"
	case CmdAddJob:
		return "CmdAddJob"
	case CmdDone:
		return "CmdDone"
	case CmdDrop:
		return "CmdDrop"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

// ==== One struct: local use + serializable ====
type RespInfo struct {
	RequestID string `msgpack:"request_id"`
	RespCh    chan<- ToProducerResponse
}

type Command struct {
	Type      CommandType
	ProposeID uint64

	// Exactly one pointer is non-nil, matching Type
	Peers      *PeersListPayload
	AddNode    *AddNodePayload
	RemoveNode *RemoveNodePayload
	Transfer   *TransferLeaderPayload
	AddJob     *AddJobPayload
	Done       *DonePayload
	Drop       *DropPayload

	RespInfo *RespInfo `msgpack:"-"` // skipped by custom marshal, nil on unmarshal
}

// ==== Payload types ====
type Job struct {
	ID    string `msgpack:"id"`
	Topic string `msgpack:"topic"`
	Data  []byte `msgpack:"data"`

	// internal use only - not for external clients
	Done      bool  `msgpack:"-"`
	CreatedAt int64 `msgpack:"-"`
}

type PeersListPayload struct {
	Peers []string `msgpack:"peers"`
}

type AddNodePayload struct {
	NodeID string `msgpack:"node_id"`
}

type RemoveNodePayload struct {
	NodeID string `msgpack:"node_id"`
}

type TransferLeaderPayload struct {
	TargetNodeID string `msgpack:"target_node_id"`
}

type AddJobPayload struct {
	Job Job `msgpack:"job"`
}

type DonePayload struct {
	Topic  string   `msgpack:"topic"`
	JobIDs []string `msgpack:"job_ids"`
}

type DropPayload struct {
	Topic  string   `msgpack:"topic"`
	JobIDs []string `msgpack:"job_ids"`
}

// ==== Wire format: [Type, ProposeID, Payload] ====
func (c Command) MarshalMsgpack() ([]byte, error) {
	var payload any

	switch c.Type {
	case CmdUpdatePeersList:
		if c.Peers == nil {
			return nil, errors.New("peers payload missing")
		}
		payload = c.Peers

	case CmdAddNode:
		if c.AddNode == nil {
			return nil, errors.New("add_node payload missing")
		}
		payload = c.AddNode

	case CmdRemoveNode:
		if c.RemoveNode == nil {
			return nil, errors.New("remove_node payload missing")
		}
		payload = c.RemoveNode

	case CmdTransferLeader:
		if c.Transfer == nil {
			return nil, errors.New("transfer_leader payload missing")
		}
		payload = c.Transfer

	case CmdAddJob:
		if c.AddJob == nil {
			return nil, errors.New("add_job payload missing")
		}
		payload = c.AddJob

	case CmdDone:
		if c.Done == nil {
			return nil, errors.New("done payload missing")
		}
		payload = c.Done

	case CmdDrop:
		if c.Drop == nil {
			return nil, errors.New("drop payload missing")
		}
		payload = c.Drop

	default:
		return nil, fmt.Errorf("unknown command type: %d", c.Type)
	}

	// Encode: [Type, ProposeID, Payload]
	return msgpack.Marshal([3]any{uint8(c.Type), c.ProposeID, payload})
}

func (c *Command) UnmarshalMsgpack(data []byte) error {
	// Decode as 3-element array
	var arr [3]msgpack.RawMessage
	if err := msgpack.Unmarshal(data, &arr); err != nil {
		return fmt.Errorf("unmarshal array: %w", err)
	}

	// Unmarshal Type
	var typeVal uint8
	if err := msgpack.Unmarshal(arr[0], &typeVal); err != nil {
		return fmt.Errorf("unmarshal type: %w", err)
	}
	c.Type = CommandType(typeVal)

	// Unmarshal ProposeID
	if err := msgpack.Unmarshal(arr[1], &c.ProposeID); err != nil {
		return fmt.Errorf("unmarshal propose_id: %w", err)
	}

	// Decode payload based on type
	switch c.Type {
	case CmdUpdatePeersList:
		c.Peers = new(PeersListPayload)
		if err := msgpack.Unmarshal(arr[2], c.Peers); err != nil {
			return fmt.Errorf("unmarshal peers: %w", err)
		}

	case CmdAddNode:
		c.AddNode = new(AddNodePayload)
		if err := msgpack.Unmarshal(arr[2], c.AddNode); err != nil {
			return fmt.Errorf("unmarshal add_node: %w", err)
		}

	case CmdRemoveNode:
		c.RemoveNode = new(RemoveNodePayload)
		if err := msgpack.Unmarshal(arr[2], c.RemoveNode); err != nil {
			return fmt.Errorf("unmarshal remove_node: %w", err)
		}

	case CmdTransferLeader:
		c.Transfer = new(TransferLeaderPayload)
		if err := msgpack.Unmarshal(arr[2], c.Transfer); err != nil {
			return fmt.Errorf("unmarshal transfer_leader: %w", err)
		}

	case CmdAddJob:
		c.AddJob = new(AddJobPayload)
		if err := msgpack.Unmarshal(arr[2], c.AddJob); err != nil {
			return fmt.Errorf("unmarshal add_job: %w", err)
		}

	case CmdDone:
		c.Done = new(DonePayload)
		if err := msgpack.Unmarshal(arr[2], c.Done); err != nil {
			return fmt.Errorf("unmarshal done: %w", err)
		}

	case CmdDrop:
		c.Drop = new(DropPayload)
		if err := msgpack.Unmarshal(arr[2], c.Drop); err != nil {
			return fmt.Errorf("unmarshal drop: %w", err)
		}

	default:
		return fmt.Errorf("unknown command type: %d", c.Type)
	}

	return nil
}
