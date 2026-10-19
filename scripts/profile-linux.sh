#!/usr/bin/env bash
set -euo pipefail
# Run from repo root, against an existing cluster. Select the current leader.
: "${PID:?Set PID to the leader process ID}"
: "${PORT:?Set PORT to the leader TCP port}"
mkdir -p profiles
case "${1:-cpu}" in
 cpu) perf record -F 99 -g -p "$PID" -o profiles/cpu.data -- python3 scripts/benchmark.py --port "$PORT" --requests 2000 --clients 4 ;;
 locks) perf lock record -p "$PID" -- sleep 15
        perf lock report > profiles/locks.txt ;;
 callgrind) echo 'Start one node with: valgrind --tool=callgrind --callgrind-out-file=profiles/callgrind.out build/raftkv serve .cluster/node0.json' ;;
 *) echo 'usage: scripts/profile-linux.sh cpu|locks|callgrind' >&2; exit 1 ;;
esac
