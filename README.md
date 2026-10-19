# Fault-Tolerant Key-Value Store

A runnable C++17 distributed key-value store with a from-scratch Raft implementation, synchronous RocksDB persistence, a TCP client, real-process failure tests, and Linux profiling tools.

## Project outline

1. **Consensus:** randomized leader elections, durable votes and terms, log matching, suffix repair, bounded AppendEntries batches, majority commit, and a current-term no-op on election.
2. **Storage:** RocksDB stores the log, metadata, and committed keys. A synchronous atomic WriteBatch persists changed log entries and state-machine effects before acknowledgements.
3. **API:** `put`, `get`, `del`, and diagnostic `status` over newline-delimited JSON. Followers return a leader hint. Every read is itself a replicated command, providing a fresh majority barrier without clock-based leases.
4. **Recovery:** restart from durable state, elect a replacement leader, and repair lagging or divergent followers. A three-node cluster tolerates one unavailable node; a minority cannot complete operations.
5. **Validation:** tests launch three actual processes and inject partitions and SIGKILL failures. Benchmark tooling reports p50/p95/p99, throughput, and failures separately.
6. **Performance:** one event loop owns Raft state, avoiding application-level consensus mutex contention. Incremental RocksDB batches avoid rewriting the full log, and replication sends at most 32 entries per RPC. Linux CPU affinity, perf, and Valgrind workflows support further measurement.

This is a learning and portfolio implementation, not a production database. See [architecture](docs/architecture.md), [protocol](docs/protocol.md), and [performance methodology](docs/performance.md) for exact guarantees and limits. See [validation results](docs/validation.md) for executed checks and the measured local baseline.

## Build and test

Requirements: Linux or macOS, C++17 compiler, CMake 3.20+, Git, Python 3.9+, and internet access for the first dependency download. Dependencies are pinned to RocksDB 9.7.3 and nlohmann/json 3.11.3 source tags.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j 4
ctest --test-dir build --output-on-failure
```

Optional application ASan/UBSan instrumentation: add `-DSANITIZE=ON` during configuration. GitHub Actions builds and runs the process tests on Linux, with and without instrumentation. Dependencies themselves are not sanitizer-instrumented by this option.

## Run an end-to-end demo

```sh
python3 scripts/cluster.py --binary build/raftkv
```

In another terminal, inspect ports 7100, 7101, and 7102 to find `"role": "leader"`:

```sh
build/raftkv client 7100 status
build/raftkv client 7101 status
build/raftkv client 7102 status
```

Use that leader's port (replace `7100` below if needed):

```sh
build/raftkv client 7100 put greeting hello
build/raftkv client 7100 get greeting
build/raftkv client 7100 del greeting
python3 scripts/benchmark.py --port 7100 --requests 1000 --clients 4
```

Press Ctrl-C in the cluster terminal to stop all three nodes. Data remains in `.cluster/`; start again to recover it. To reset, stop every node and remove that cluster directory. The CLI does not automatically retry or follow redirects: inspect status and send to the current leader.

## Separate hosts

Run `build/raftkv serve node.json` on each host. Use a unique ID and data directory, the same fixed membership everywhere, and reachable IPv4 addresses:

```json
{
  "id": 0,
  "bind": "10.0.0.10",
  "port": 7100,
  "data_dir": "./node0-data",
  "peers": [
    {"id": 1, "host": "10.0.0.11", "port": 7100},
    {"id": 2, "host": "10.0.0.12", "port": 7100}
  ]
}
```

On node 1, use ID 1, its local bind address, and peers 0 and 2; likewise for node 2. Omitted addresses default to loopback. Connect using `build/raftkv client 10.0.0.10:7100 status`. IPv4 literals are supported; DNS names and IPv6 are not. The included automated tests run separate processes on loopback, not separate physical hosts.

Peer and client traffic is unauthenticated and shares the same listener, including the test-only `isolate` endpoint. Use only a trusted private lab network. Membership is static; do not change IDs or peer lists on existing data directories.

## Failure semantics

Successful writes are durable on a majority. A leader serves a successful read only after its read command is committed by a majority. A partitioned minority returns `unavailable`; the majority may elect a leader and continue. An `unavailable` response or a dropped connection has an **unknown outcome**: an appended operation can commit later. There is no request deduplication or exactly-once execution; repeated assignments/deletions are naturally idempotent, but retries across intervening writes can change the result.

No snapshots, log compaction, dynamic membership, TLS, authentication, automatic client failover, or production monitoring are implemented. Logs remain in memory and on disk and grow with reads as well as writes. Serialized peer RPCs and synchronous disk writes limit throughput. Timing assumes a small, low-latency cluster; configurable transport scheduling and higher-latency network testing are future work.

## References

- [Raft paper](https://raft.github.io/raft.pdf): consensus and safety rules.
- [RocksDB basic operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations): atomic batches and synchronous writes.

No Linux performance improvement percentage is claimed. Reproduce measurements on your target machine before using quantitative resume claims.
