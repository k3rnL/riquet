# PVC storage: bbolt

Status: accepted for v1

Riquet's PVC profile uses `go.etcd.io/bbolt` v1.3.11. This is the newest
selected release whose module baseline matches Riquet's Go 1.22 baseline. It is
pure Go, uses one database file, provides ACID write transactions and consistent
read transactions, takes an exclusive process lock, and exposes transactionally
consistent physical backup through `Tx.WriteTo`.

Persistent format version 1 contains metadata and ordered transition buckets.
One bbolt update transaction writes a transition and advances the committed
sequence. Snapshots are optional replay accelerators and never move beyond the
committed sequence. An exact transition retry is idempotent; a conflicting
record at the same sequence or a sequence gap is rejected without mutation.

The database is opened with synchronous commits in normal operation. Tests may
use `NoSync` only for isolated benchmarks. Startup rejects unknown format
versions, competing writers fail within a configured lock timeout, and replay
validates JSON, sequence continuity, envelope version, and checksum.

An August 2026 development run on Linux/amd64 with an AMD Ryzen 5 9600X and
Go 1.22.2 measured `BenchmarkCommitDurable` at 5.94 ms/op (261 iterations) and
the explicitly test-only `BenchmarkCommitNoSync` at 26.7 us/op (46,860
iterations). These figures are local signals, not product guarantees. Release
qualification will replace them with durable-storage and Kubernetes-volume
measurements; the architectural choice depends on correctness and recovery
rather than a particular local IOPS number.

Recovery tests cover clean reopen, forced process-level lock contention,
physical-backup reopen, exact retry, sequence gaps, and corrupt transition
payloads. The common conformance suite additionally checks callback failure,
snapshot validation, capabilities, and health.
