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

//go:build integration

package test

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m-javani/cue/internal/state"
)

func BenchmarkPartition(b *testing.B) {
	jobCounts := []int{10000} // Start with 10000

	for _, count := range jobCounts {
		b.Run(fmt.Sprintf("jobs_%d", count), func(b *testing.B) {
			benchmarkPartition(b, count)
		})
	}
}

func benchmarkPartition(b *testing.B, numJobs int) {
	// Create config with large capacity
	config := state.DefaultPartitionConfig()
	config.DispatchBatchSize = 1024
	config.ActiveQueueCapacity = 100000

	// Create tester with auto-done enabled
	tester := state.NewPartitionTester("bench-topic", config)
	defer tester.Cleanup()

	// Send heartbeats to make proxy available
	stopHeartbeats := make(chan struct{})
	defer close(stopHeartbeats)
	go tester.SendHeartbeats(100, 100*time.Millisecond, stopHeartbeats)

	// Give partition time to become ready
	time.Sleep(500 * time.Millisecond)

	// Pre-allocate job IDs
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("job-%d", i)
	}
	jobData := []byte(`{"payload":"test"}`)

	b.ResetTimer()

	for iter := 0; iter < b.N; iter++ {
		startTime := time.Now()

		// Track latencies
		latencies := make([]time.Duration, 0, numJobs)
		var latenciesMu sync.Mutex

		// Worker pool for adding jobs
		numWorkers := 100
		var wg sync.WaitGroup
		jobCh := make(chan int, numJobs)

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for idx := range jobCh {
					createdAt := time.Now()
					tester.AddJobAsync(jobIDs[idx], jobData)

					latenciesMu.Lock()
					latencies = append(latencies, time.Since(createdAt))
					latenciesMu.Unlock()
				}
			}()
		}

		// Send jobs
		for i := 0; i < numJobs; i++ {
			jobCh <- i
		}
		close(jobCh)
		wg.Wait()

		// Wait for all responses
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			success, errors, _, _, _ := tester.GetCounts()
			if int(success+errors) >= numJobs {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		// Wait for all jobs to be dispatched
		if !tester.WaitForDispatched(numJobs, 30*time.Second) {
			success, errors, dispatched, _, _ := tester.GetCounts()
			b.Logf("Success: %d, Errors: %d, Dispatched: %d", success, errors, dispatched)
			b.Fatalf("timeout waiting for %d jobs to be dispatched", numJobs)
		}

		// Give auto-done processor time to send final Done commands
		time.Sleep(100 * time.Millisecond)

		elapsed := time.Since(startTime)

		// Report metrics
		b.ReportMetric(float64(numJobs)/elapsed.Seconds(), "throughput_jobs/sec")

		if len(latencies) > 0 {
			sort.Slice(latencies, func(i, j int) bool {
				return latencies[i] < latencies[j]
			})

			p50 := percentileDuration(latencies, 0.50)
			p95 := percentileDuration(latencies, 0.95)
			p99 := percentileDuration(latencies, 0.99)
			max := latencies[len(latencies)-1]

			b.ReportMetric(float64(p50.Microseconds()), "p50_us")
			b.ReportMetric(float64(p95.Microseconds()), "p95_us")
			b.ReportMetric(float64(p99.Microseconds()), "p99_us")
			b.ReportMetric(float64(max.Microseconds()), "max_us")

			b.Logf("Jobs: %d, Throughput: %.0f jobs/sec, P50: %v, P95: %v, P99: %v, Max: %v",
				numJobs, float64(numJobs)/elapsed.Seconds(), p50, p95, p99, max)
		}

		success, errors, dispatched, dropped, _ := tester.GetCounts()
		b.Logf("Success: %d, Errors: %d, Dispatched: %d, Dropped: %d",
			success, errors, dispatched, dropped)
	}
}

func percentileDuration(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}
