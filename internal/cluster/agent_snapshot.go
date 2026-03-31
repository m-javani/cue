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
	"time"

	"go.uber.org/zap"
)

// runSnapshotMaintenance starts a goroutine that periodically triggers snapshots
func (a *ClusterAgent) runSnapshotMaintenance() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return

		case <-ticker.C:
			// Only active nodes (leader or follower) should snapshot
			if !a.IsActive() {
				continue
			}

			if a.ShouldSnapshot() {
				a.lastSnapshotTrySec.Store(time.Now().Unix())
				if err := a.saveSnapshot(); err != nil {
					a.logger.Error("failed to save snapshot", zap.Error(err))
				}
			}
		}
	}
}

// saveSnapshot triggers a snapshot via Raft
func (a *ClusterAgent) saveSnapshot() error {
	a.lastSnapshotTrySec.Store(time.Now().Unix())

	// Send snapshot command to Raft via control channel
	select {
	case a.ctrlCh <- ControlCmd{
		Type: CmdSnapshot,
	}:
		return nil
	case <-a.ctx.Done():
		return a.ctx.Err()
	default:
		a.logger.Warn("snapshot channel full, will retry later")
		return nil
	}
}

// ShouldSnapshot checks if a snapshot should be triggered
func (a *ClusterAgent) ShouldSnapshot() bool {
	currentSec := time.Now().Unix()

	// Cooldown lock - prevent trying again too soon
	lastTrySec := a.lastSnapshotTrySec.Load()
	if lastTrySec > 0 && (currentSec-lastTrySec) < int64(a.snapshotIntervalSec) {
		return false
	}

	// WAL gap trigger (safeguard)
	lastApplied := a.lastAppliedIndex.Load()
	snapshotIndex := a.snapshotIndex.Load()
	walGap := lastApplied - snapshotIndex

	if walGap >= a.snapshotTriggerCount {
		return true
	}
	if walGap == 0 {
		return false
	}

	// Time-based trigger (primary)
	lastSnapshotSec := a.lastSnapshotSec.Load()
	timeSinceLast := currentSec - lastSnapshotSec
	if lastSnapshotSec > 0 && timeSinceLast >= int64(a.snapshotIntervalSec) {
		return true
	}

	return false
}
