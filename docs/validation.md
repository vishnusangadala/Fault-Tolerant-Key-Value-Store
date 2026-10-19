# Development validation

Validated on 2026-10-19 on macOS 27.0.1, arm64, using Apple Clang and a Release CMake build. The local environment did not expose the CPU model through its restricted system query. Dependencies: RocksDB 9.7.3, nlohmann/json 3.11.3. Three processes used loopback and separate RocksDB directories on the host filesystem.

## Automated scenarios

- Leader election and follower redirect responses.
- PUT/GET/DELETE, empty keys, escaped newline/NUL/Unicode values, invalid client requests.
- Isolated old leader cannot complete reads or writes.
- Majority elects a replacement and continues writing.
- Healing repairs the old leader's conflicting uncommitted suffix.
- SIGKILL leader failover and restarted follower catch-up.
- Full-cluster SIGKILL and durable restart.
- Catch-up beyond one 32-entry replication batch.
- Quorum loss rejects operations; restoring a majority permits progress.
- Vote durability across crashes; no second candidate vote in the same term.
- Candidate log freshness compares last term before index.
- Stale leaders and mismatched predecessor terms are rejected.
- Follower commit advancement is bounded by the prefix matched in the current AppendEntries.

Both CTest suites are also run with ASan/UBSan enabled for the application. The process tests use real TCP sockets and real RocksDB, not a mock storage engine. Instrumentation excludes third-party dependency internals.

## One illustrative local baseline

A fresh three-node cluster, 500 successful PUT requests, 4 closed-loop clients, 128-byte values, 100 rotating keys, one TCP connection per request, no explicit warm-up or CPU pinning:

| Metric | Observed |
|---|---:|
| Successful requests | 500 |
| Failures | 0 |
| Elapsed | 12.179 seconds |
| Successful throughput | 41.05 ops/s |
| p50 | 94.88 ms |
| p95 | 124.60 ms |
| p99 | 145.81 ms |
| Maximum | 153.11 ms |

These are a single macOS run with synchronous durability, performed while other development tasks were active. They are not Linux numbers, an improvement comparison, a capacity claim, or a cross-machine result. See `benchmark-local.json` for machine-readable output and [performance.md](performance.md) to reproduce better controlled measurements. High per-operation cost is expected from per-request synchronous durability, serialized replication, and TCP connection setup; no low-latency production target is asserted.

Linux CI is configured to repeat the automated process suites; its actual run status must be checked on GitHub. Linux affinity, perf, and Valgrind workflows have not been run on this macOS host. Separate-host deployment is configurable but has not been exercised here. No formal proof, exhaustive schedule exploration, power-loss test, or adversarial network linearizability audit has been performed.
