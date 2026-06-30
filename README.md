# Cue

**Cue** is a distributed, in-memory job queue with Raft-based consistency, automatic retries, and dead letter handling.

Cue is for teams who need reliable job dispatch without operating complex infrastructure. It's not a Kafka replacement - it's an honest, bounded, operationally simple alternative for teams that need durable, retrying job queues.

> **⚠️ Cue is a distributed system.** A single node has no durability. For production, run 3, 5, or 7 nodes. CueProxy is the companion gateway - see [cue-proxy](https://github.com/m-javani/cue-proxy).

---

[![CI](https://github.com/m-javani/cue/actions/workflows/ci.yml/badge.svg)](https://github.com/m-javani/cue/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/m-javani/cue/graph/badge.svg)](https://app.codecov.io/gh/m-javani/cue)
[![Go Report Card](https://goreportcard.com/badge/github.com/m-javani/cue)](https://goreportcard.com/report/github.com/m-javani/cue)
[![Go Version](https://img.shields.io/badge/Go-1.26.3-blue)](https://golang.org/)
[![Docker Pulls](https://img.shields.io/docker/pulls/mehdyjavany/cue)](https://hub.docker.com/r/mehdyjavany/cue)

---

## Table of Contents

- [Full Docs](#docs)
- [How It Works](#how-it-works)
- [Key Features](#key-features)
- [Limitations](#limitations)
- [Contribution](#contributing)
- [License](#license)

---

## Docs 
> read the full docs [here](https://m-javani.github.io/cue-docs/).

**Related Project:**
- [CueProxy](https://github.com/m-javani/cue-proxy) - The stateless HTTP/WebSocket gateway
---

## How It Works

```
                    ┌─────────────────────────────────────────────────┐
                    │               Cue Cluster                      │
                    │                                                │
                    │  ┌─────────────┐  ┌─────────────┐  ┌─────────┐│
                    │  │   Leader    │  │  Follower   │  │Follower ││
                    │  │   node1     │  │   node2     │  │ node3   ││
                    │  │             │  │             │  │         ││
                    │  │ ┌─────────┐ │  │ ┌─────────┐ │  │┌───────┐││
                    │  │ │Partition│ │  │ │Partition│ │  ││Partit.│││
                    │  │ │  A      │ │  │ │  A      │ │  ││ A     │││
                    │  │ │  B      │ │  │ │  B      │ │  ││ B     │││
                    │  │ │  C      │ │  │ │  C      │ │  ││ C     │││
                    │  │ └─────────┘ │  │ └─────────┘ │  │└───────┘││
                    │  │    WAL      │  │    WAL      │  │  WAL    ││
                    │  └─────────────┘  └─────────────┘  └─────────┘│
                    │         ▲                ▲              ▲     │
                    │         └────────────────┴──────────────┘     │
                    │                   Raft Replication             │
                    └─────────────────────────────────────────────────┘
                                          │
                                QUIC      │      QUIC
                    ┌─────────────────────┼─────────────────────┐
                    │                     │                     │
              ┌─────▼─────┐         ┌─────▼─────┐         ┌────▼────┐
              │ CueProxy  │         │ CueProxy  │         │CueProxy │
              │ Instance  │         │ Instance  │         │Instance │
              └───────────┘         └───────────┘         └─────────┘
```

**All nodes are identical** - every node has the same set of partitions (topics). Raft keeps them in sync:

- **Leader**: Accepts writes, replicates to followers, dispatches jobs to proxies
- **Followers**: Receive replicated commands via Raft, apply to local partitions

**Partitions are internal** - each topic is a separate partition with its own:
- In-memory job queue
- Retry scheduler
- Dead Letter Queue
- Consumer state tracking

**All nodes see the same state** - Raft ensures every command (add job, done, drop) is applied identically on all nodes. If the leader has 3 topics (A, B, C), all followers have the exact same 3 topics with the same jobs.

**Cue cluster nodes communicate via QUIC** with mTLS authentication. Each node runs:
- **Raft consensus** - for leader election and command replication
- **Partition manager** - each topic is a separate in-memory partition
- **WAL** - Write-Ahead Log for durability and recovery
- **QUIC gateway** - accepts connections from CueProxy instances

**The flow:**
1. Producers submit jobs via CueProxy
2. The Cue cluster leader persists jobs to Raft WAL and memory
3. The leader dispatches jobs to proxies with consumers for that topic
4. Consumers acknowledge completion via proxy
5. The leader removes completed jobs from memory

**Consistency:**
- All writes go through the leader
- Raft ensures linearizable consistency
- WAL provides durability across restarts
- Snapshots prevent log growth

**Resilience:**
- Automatic leader election on failure
- Jobs survive leader crashes (WAL replay on restart)
- Retries with exponential backoff
- Dead Letter Queue for failed jobs

## Key Features

### 🏛️ Raft-Based Consistency
No split-brain. No data loss. Raft consensus ensures all nodes agree on state.

### 💾 Durable by Default
Write-Ahead Log persists every command. Crashes and restarts replay the WAL.

### 🔁 Automatic Retries
Jobs retry with exponential backoff (configurable). Max retries and backoff limits prevent infinite loops.

### 💀 Dead Letter Queue
Failed jobs go to DLQ with configurable retention (size and age limits).

### 📦 In-Memory Partitions
One partition per topic. Fast, bounded, predictable. No disk I/O for job dispatch.

### 🔐 Secure QUIC Communication
mTLS between cluster nodes. TLS between cluster and proxies. Configurable certificate verification.

### 📊 Built-in Monitoring
Prometheus metrics for partitions, cluster, and gateway.

## Limitations

Cue is designed to be honest about its limits:

- **In-memory only**: Jobs are stored in memory. Raft WAL provides durability across restarts, but total capacity is bounded by available RAM.
- **Bounded queues**: Configurable `active_queue_capacity` per topic (default: 1,000,000). Exceeding this returns "queue full" errors.
- **No replay**: Once acknowledged, jobs are removed from memory. No historical replay.
- **Leader-dependent writes**: All write operations go through the leader. Followers are read-only.
- **Throughput**: Limited by leader's memory, Raft consensus speed, and network latency.
- **No SQL/query**: Jobs are opaque byte arrays. No filtering or searching.

**Cue is not for:**
- Large-scale persistent streaming (use Kafka)
- Fire-and-forget pub/sub (use Redis)
- Long-term job storage (use a database)
- Complex job dependencies (use a workflow engine)

**Cue is for:**
- Teams needing reliable job dispatch without complex ops
- At-least-once delivery with automatic retries
- Simple, bounded, predictable job queues
- Internal microservices communication

## Contributing

Contributions are welcome! Please read the [contributing guide](CONTRIBUTING.md) for details.

## License

MIT License - see [LICENSE](LICENSE) for details.

---
