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

package internal

import "errors"

var (
	// ErrServiceUnavailable returned when node is not active (not leader or follower)
	ErrServiceUnavailable = errors.New("service unavailable: node is not active")

	// ErrUnknownCommand returned when command type is not recognized
	ErrUnknownCommand = errors.New("unknown command type")

	// ErrUnknownRequestType returned when cluster request type is not recognized
	ErrUnknownRequestType = errors.New("unknown request type")

	// ErrInvalidRaftMessage returned when Raft message decoding fails
	ErrInvalidRaftMessage = errors.New("invalid raft message")

	// ErrDeadlineExceeded returned when request exceeds deadline
	ErrDeadlineExceeded = errors.New("deadline exceeded")

	// ErrMaxRetriesExceeded returned when max retries are exhausted
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")

	ErrUnknownNodeID = errors.New("unknown node ID for raft ID")

	ErrDuplicateJobID = errors.New("job ID already exist")

	ErrInvalidPayload = errors.New("invalid payload")

	ErrQueueFull = errors.New("queue is full")

	ErrTopicNotFound = errors.New("topic not found")
	ErrTopicExists   = errors.New("topic already exists")
)
