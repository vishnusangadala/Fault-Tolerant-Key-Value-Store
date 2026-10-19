# Performance and profiling

## Reproducible measurement

Build `RelWithDebInfo`, start three nodes, locate the leader, warm up with a short benchmark, then measure using identical workloads and a fresh data directory for each comparison:

```sh
python3 scripts/benchmark.py --port 7100 --requests 200 --clients 1
python3 scripts/benchmark.py --port 7100 --requests 5000 --clients 1
python3 scripts/benchmark.py --port 7100 --requests 5000 --clients 4
```

The output contains successful client-observed p50/p95/p99/max milliseconds, success throughput, elapsed time, request size, concurrency, and failure count. Failures are excluded from latency quantiles but explicitly counted. A process restart or leader change requires choosing the current leader again. This is a closed-loop workload, so it can understate tail latency under overload (coordinated omission). Repeat runs, retain raw JSON, and compare equal successful workloads and error rates.

Record CPU model, OS/kernel, compiler, build type, RocksDB version, filesystem/device, membership placement, concurrency, values, run length, and node CPU placement. Local multi-process results include shared-host interference and do not represent a cross-machine cluster.

## Linux CPU affinity

A node config may contain `"cpu": 2`; Linux calls `sched_setaffinity` before opening RocksDB, and background threads inherit the mask. Invalid/unavailable CPUs fail startup. On macOS the field has no effect. Choose allowed CPUs using your cgroup/affinity mask; do not assume CPU 0 is available. Pin different nodes to distinct cores and compare against unpinned runs. Affinity can improve or worsen latency; measure it.

Alternatively launch a node with `taskset -c 2 build/raftkv serve .cluster/node0.json`. Compare repeated unpinned/pinned runs with identical data and workload. Do not change multiple factors simultaneously.

## CPU and lock profiling

```sh
PID=1234 PORT=7100 bash scripts/profile-linux.sh cpu
perf report -i profiles/cpu.data
PID=1234 PORT=7100 bash scripts/profile-linux.sh locks
```

The CPU mode runs its own load; lock mode records for 15 seconds, so run a benchmark concurrently in another terminal. `perf lock` observes kernel lock events, **not all pthread mutex contention**. Availability depends on kernel tracepoint support and perf permissions. For user-space contention, use an appropriate futex/off-CPU profiler or inspect stacks with platform tooling. The application has no shared Raft mutex; contention can still occur inside RocksDB, the allocator, and the kernel. Do not claim reduced lock wait time without a baseline and appropriate measurements.

## Valgrind on Linux

Stop the node first, then run it under Memcheck or Callgrind using the same data/config:

```sh
mkdir -p profiles
valgrind --tool=memcheck --leak-check=full --error-exitcode=99 \
  build/raftkv serve .cluster/node0.json
valgrind --tool=callgrind --callgrind-out-file=profiles/callgrind.out \
  build/raftkv serve .cluster/node0.json
callgrind_annotate profiles/callgrind.out
```

Instrumentation can slow a node enough to cause elections; profiling results are diagnostic, not normal latency measurements. A forced SIGKILL cannot produce a normal final Valgrind report. Memcheck/Callgrind and Linux perf are supplied as reproducible workflows; they were not executed on the macOS development host.

## Implemented choices and future experiments

Incremental durable batches avoid rewriting the entire log per operation. A single Raft owner avoids application consensus-lock contention. Bounded AppendEntries batches cap per-message catch-up work. These are structural choices, not demonstrated percentage improvements. Potential experiments include parallel peer RPCs (to remove slow-peer head-of-line delay), write group commit, ReadIndex, snapshots, and adaptive batching. Each requires new safety tests. Report actual before/after p99 and error rates when making performance claims.
