# Project 21: Simplified Log Replication System

This project implements a compact RAFT-style replicated log. A single leader accepts client writes, stores entries in SQLite, replicates them to followers, commits after a majority acknowledgement, applies committed entries to a deterministic in-memory state machine, and elects a new leader after fail-stop crashes.

## Architecture

Each node is the same Go binary and contains:

| Component | Responsibility | Implementation |
| --- | --- | --- |
| Replication engine | Owns RAFT role transitions, client writes, AppendEntries, commit advancement, backfill, and state-machine application. | Single event loop with channels from gRPC handlers. |
| Election manager | Tracks `currentTerm`, `votedFor`, randomized election timeout, RequestVote RPCs, and leader transitions. | Persisted term/vote in SQLite before replies. |
| Log store | Stores log entries and persistent metadata. | SQLite with WAL and `synchronous=FULL`. |
| State machine | Deterministically applies committed commands. | Mutex-protected `map[string]string`. Supports `SET key value` and `DEL key`; unknown commands are stored as `entry/<index>`. |
| gRPC/protobuf layer | Inter-node RAFT RPCs and client API. | `RequestVote`, `AppendEntries`, `SubmitEntry`, `QueryLog`, `GetMetrics`. |
| Metrics collector | Records submit, commit, and apply timestamps. | Mutex-protected timestamp maps exposed through `GetMetrics`. |

## Repository Layout

```text
cmd/node/              RAFT node server
cmd/client/            CLI for submit/query/metrics/compare/validate
internal/raft/         RAFT logic, SQLite store, state machine, metrics
internal/pb/           Committed protobuf-compatible Go stubs
proto/raft.proto       RPC and message schema
docker-compose.yml     Three-node local cluster
scripts/validate.*     End-to-end validation helpers
```

The protobuf-compatible Go files are committed so normal builds do not require `protoc`. If `proto/raft.proto` changes, regenerate or update `internal/pb`.

## Quick Start

With Docker:

```sh
docker compose up -d --build node1 node2 node3
docker compose run --rm client validate --nodes "node1:50051,node2:50051,node3:50051"
```

From the host after the cluster is running:

```sh
go run ./cmd/client submit --nodes localhost:5001,localhost:5002,localhost:5003 --command "SET color blue"
go run ./cmd/client compare --nodes localhost:5001,localhost:5002,localhost:5003
go run ./cmd/client metrics --addr localhost:5001
```

Useful Make targets:

```sh
make up
make submit CMD="SET user alice"
make compare
make validate
make crash
make restart
make down
```

## Validation Checklist

### Submit log entries and compare logs across nodes

```sh
docker compose run --rm client submit --nodes "node1:50051,node2:50051,node3:50051" --command "SET x 1"
docker compose run --rm client compare --nodes "node1:50051,node2:50051,node3:50051"
```

`compare` queries each node through `QueryLog` and compares `(index, term, command)` sequences.

### Simulate node crash/restart and verify continuity

```sh
docker compose stop node2
docker compose run --rm client submit --nodes "node1:50051,node2:50051,node3:50051" --command "SET during_crash yes"
docker compose start node2
sleep 4
docker compose run --rm client compare --nodes "node1:50051,node2:50051,node3:50051"
```

The restarted node reloads `currentTerm`, `votedFor`, `commit_index`, and log entries from SQLite. The leader uses `nextIndex` retries to backfill missing entries.

### Measure log application delay and replication lag

```sh
docker compose run --rm client validate --nodes "node1:50051,node2:50051,node3:50051" --command "SET metric sample"
docker compose run --rm client metrics --addr node1:50051
```

Metrics include:

- `replication_lag = commitIndex - lastApplied`
- `submitted_at_unix_nano`
- `committed_at_unix_nano`
- `applied_at_unix_nano`
- `apply_delay_nanos`

The `validate` command submits an entry, waits for log convergence, and prints per-node delay from the leader commit timestamp to each node apply timestamp.

## API

`RaftService` exposes:

- `RequestVote(RequestVoteRequest)`: election RPC.
- `AppendEntries(AppendEntriesRequest)`: heartbeat and log replication RPC.
- `SubmitEntry(SubmitEntryRequest)`: client write API. Followers reject writes and return the known leader id when available.
- `QueryLog(QueryLogRequest)`: returns role, term, commit index, applied index, log entries, and state snapshot.
- `GetMetrics(GetMetricsRequest)`: returns lag and per-entry timing data.

See [proto/raft.proto](proto/raft.proto) for exact message schemas.

## Local Non-Docker Run

Start three terminals:

```sh
go run ./cmd/node --id node1 --addr :5001 --data ./data/node1 --peers "node1=localhost:5001,node2=localhost:5002,node3=localhost:5003"
go run ./cmd/node --id node2 --addr :5002 --data ./data/node2 --peers "node1=localhost:5001,node2=localhost:5002,node3=localhost:5003"
go run ./cmd/node --id node3 --addr :5003 --data ./data/node3 --peers "node1=localhost:5001,node2=localhost:5002,node3=localhost:5003"
```

Then:

```sh
go run ./cmd/client validate --nodes localhost:5001,localhost:5002,localhost:5003
```

## Consistency Notes

- Writes are accepted only by the leader.
- Entries are committed after a majority acknowledgement.
- Followers enforce `prevLogIndex` and `prevLogTerm` checks.
- Candidates must have an up-to-date log to win an election.
- Committed entries are replayed from SQLite on restart.
- Leader recovery/backfill uses `nextIndex` decrement and AppendEntries retries.
