## Context

The repository contains no application implementation yet. The public contract is the Confluent Schema Registry REST surface and its observable schema semantics, while deployment must remain independent of a particular Kafka distribution. The same process must support a durable single-node profile and a Kafka-backed multi-replica profile. See `proposal.md` and the six capability specs for scope and behavior.

The hardest constraints are atomic global ID and subject-version allocation, exact format-specific compatibility, replayable recovery, safe leader failover, and repeatable comparison against a moving external reference implementation.

## Goals / Non-Goals

**Goals:**

- Keep protocol handling, registry state transitions, schema engines, and persistence independently testable.
- Make every accepted mutation a deterministic atomic state transition that can be replayed.
- Use one service binary and one public API in both PVC and Kafka-backed modes.
- Make the Kafka-backed service resilient to replica loss and rolling replacement without losing acknowledged mutations.
- Treat compatibility evidence and declared exceptions as versioned product artifacts.
- Preserve a path for future storage backends and Confluent extensions without embedding them into the v1 domain model prematurely.

**Non-Goals:**

- A generic distributed database abstraction or arbitrary external storage plug-ins loaded at runtime.
- Linearizable reads from every follower immediately after every write; ready followers converge through ordered replay and are removed from routing when unsafe lag is detected.
- Reimplementing Confluent client serializers or the Kafka record envelope; compatibility is obtained by serving existing clients correctly.
- Active-active writes on the initial PVC backend.
- Cross-registry linking, governance rules, alternate registry protocols, or a management UI in v1.

## Decisions

### 1. Use a layered Go service with a deterministic domain core

The service will be split into transport adapters, a registry application/domain layer, format engines, storage/coordination adapters, and operational packaging. HTTP handlers translate requests into typed commands and translate typed results into Confluent response shapes. The domain core alone decides validation order, identity, compatibility, ID allocation, version allocation, reference constraints, deletion, configuration, and modes.

The core receives an immutable current view plus a command and produces either a domain error or an ordered transition with its result. It performs no network or disk I/O. This permits the exact same scenario to run against in-memory models, PVC storage, and Kafka storage.

Alternatives considered:

- Putting behavior in HTTP handlers is initially faster but makes retries, replay, and differential testing inconsistent.
- Modeling storage as generic CRUD obscures the atomic boundary across IDs, versions, references, and configuration.

### 2. Persist ordered state transitions and maintain materialized views

Both backends will expose an ordered durable transition stream or an equivalent transactional sequence. Each process serves reads from a materialized registry view reconstructed from a consistent snapshot plus later transitions. Persistent records use a versioned internal envelope, checksum, operation identifier, logical sequence, and payload version.

Transitions will describe domain changes rather than serialized HTTP requests. Replaying a committed transition therefore does not repeat parsing, compatibility checks, or ID allocation. Periodic snapshots are optimizations only and include the applied sequence/checksum needed to resume safely.

Alternatives considered:

- Replaying client commands could produce different results after libraries or compatibility behavior change.
- Persisting only ad hoc tables makes the two backends behave differently and makes deterministic recovery testing harder.

### 3. Define storage around transactions, replay, and declared capabilities

The internal storage boundary will support opening and validating a store, loading a consistent snapshot, replaying after a sequence, atomically committing a fenced transition, creating a backup, and reporting health/capabilities. Coordination is a separate companion interface so single-writer storage is not forced to emulate distributed leadership.

Backend capabilities are checked during startup and by Helm validation. The interface is compiled into the binary; v1 does not expose a third-party plug-in ABI. A shared conformance suite tests all declared behaviors.

Alternatives considered:

- A `Get`/`Put` interface is too weak to guarantee atomic registration.
- A runtime Go plug-in ABI would constrain builds and security without helping the two initial backends.

### 4. Use a transactional embedded store for PVC mode

PVC mode will use a pure-Go transactional embedded database in a single file plus an exclusive process lock. The initial implementation will prefer bbolt unless an early persistence spike demonstrates a compatibility, corruption-recovery, or performance blocker. Registry reads continue to use the in-memory materialized view; the embedded database stores transition/snapshot metadata and provides consistent online backup.

The Helm chart permits one active replica for this profile. Restart or Kubernetes rescheduling can reattach the volume, but the profile is not marketed as zero-downtime HA.

Alternatives considered:

- SQLite provides excellent tooling but introduces a CGO or alternative-driver decision that is unnecessary for an ordered key/value state model.
- Flat files complicate crash-safe atomic commits and online backup.
- RWX filesystem locking is too environment-dependent to form an active-active guarantee.

### 5. Use Kafka as durable log, coordination system, and fencing boundary in HA mode

Kafka mode uses a configurable compacted internal topic with one ordered registry-state partition in v1. Every Riquet replica consumes the full state stream into its local materialized view rather than sharing read partitions. A dedicated single-partition coordination group elects one primary. The new primary initializes a stable transactional producer identity, fencing any obsolete producer epoch before accepting writes.

Any public replica may receive a mutation. A follower discovers the primary through a fenced coordination record and forwards the typed internal request over authenticated internal HTTP. The primary evaluates the command against its caught-up state, commits transition records transactionally, observes the committed result, and only then returns success. Operation identifiers make ambiguous retries safe.

The single state partition deliberately prioritizes deterministic Confluent-compatible global ordering over horizontal write throughput. Format parsing can occur concurrently, but mutation validation and commit are serialized. Partitioning by context or tenant can be introduced with contexts in a later compatibility level.

Alternatives considered:

- Allowing every replica to allocate IDs would require consensus or change externally visible ID behavior.
- A separate etcd dependency would violate the standalone deployment goal.
- A shared Kafka consumer group for materialization would give each replica only part of the registry state.

### 6. Serve reads locally with lag-aware readiness

Reads are served from immutable snapshots of each replica's materialized view. A successful mutation is immediately visible on the primary that handled it; followers converge after consuming the committed transition. Each replica tracks the Kafka committed position and its applied position. It becomes non-ready when replay is incomplete, storage is unavailable beyond tolerance, or lag exceeds a configurable threshold.

This gives scalable low-latency reads and honest readiness while accepting bounded follower staleness. Users requiring immediate read-after-write can use a client retry window; future routing affinity or primary-read modes can strengthen this without changing stored data.

Alternatives considered:

- Routing every read through the primary reduces availability and wastes materialized replicas.
- Claiming strong reads through a load-balanced service would be inaccurate without a request consistency token or additional consensus read.

### 7. Isolate schema behavior behind format engines

Avro, Protobuf, and JSON Schema each implement a common engine contract for parse, validate, canonical identity, normalize, resolve references, and compatibility comparison. Schema identity includes the declared type and normalized reference identity required by the reference behavior. Reference resolution is performed against an immutable domain view with depth, count, cycle, and work limits.

Third-party parsers will be selected and pinned through focused corpus spikes. Confluent-specific compatibility and normalization behavior remains in Riquet-owned adapters so upstream library upgrades cannot silently redefine the public contract.

Alternatives considered:

- One generalized schema representation loses wire-format-specific evolution semantics.
- Delegating all compatibility behavior to libraries makes exact reference parity and stable upgrades unlikely.

### 8. Treat the API mapper as a versioned compatibility layer

Routes, request decoding, media negotiation, query defaults, validation order, errors, and response encoding live in a Confluent v1 adapter. Domain errors have stable categories; the adapter maps them to endpoint-specific statuses and numeric Confluent errors. Unsupported advanced fields are rejected or handled exactly as recorded in the compatibility manifest rather than silently discarded.

The initial declared baseline is a pinned Confluent Platform release selected when the differential harness is bootstrapped. Required CI remains pinned, while a scheduled job runs the newest supported release line and produces drift reports. Exact baseline image versions and exceptions live in a machine-readable manifest, not this design document.

Alternatives considered:

- Coding directly from documentation misses validation order and edge-case behavior.
- Following `latest` in required CI makes the Riquet contract change without review.

### 9. Build one black-box scenario model for Riquet and Confluent

The end-to-end harness provisions isolated endpoints and executes a versioned scenario corpus through HTTP and real clients. It captures normalized traces and compares results plus subsequent observable state. A symbolic mapping relates independently allocated IDs only in tests where their exact numeric value is not itself under test.

Test layers are unit/domain transitions, format corpora, storage conformance, API contract, differential E2E, real-client SerDes, concurrency, and fault/recovery. Confluent images are acquired only in the test environment under their applicable terms and are not redistributed with Riquet.

### 10. Keep operational interfaces backend-neutral

The binary accepts configuration from YAML, environment variables, and flags with documented precedence. Public HTTP, internal forwarding, metrics, and health listeners can be independently bound. TLS can terminate in-process or at an ingress. Authentication middleware supports anonymous, Basic, and validated bearer-token modes; v1 does not implement Confluent enterprise RBAC semantics.

Backup uses a backend-neutral logical snapshot envelope so identifiers and relationships can move between backend types. PVC can additionally use consistent physical backup; Kafka remains authoritative through its topic, while explicit logical export is used for portable backup and restore.

The Helm chart encodes backend profiles, probes, security contexts, secrets, network policy examples, a disruption budget for Kafka mode, and a values schema that rejects unsupported combinations.

## Risks / Trade-offs

- [Exact compatibility is broader than documented endpoints] → Bootstrap differential tests before implementing breadth, version the reference baseline, and require explicit exception records.
- [Format libraries disagree with Confluent in edge cases] → Own the compatibility adapters, pin dependencies, and use a mutation corpus against the reference implementation.
- [A single Kafka partition limits mutation throughput] → Measure before optimizing; registry writes are normally low volume, and deterministic global ID allocation is more important in v1.
- [Follower reads can briefly lag a successful mutation] → Document bounded convergence, expose lag, remove stale replicas from readiness, and retain a future primary-read option.
- [Kafka transactional and group behavior differs across compatible brokers] → Run the storage/HA suite against a declared broker matrix and publish supported minimum capabilities.
- [A compacted topic can lose reconstructive information if record keys are wrong] → Version the log model, test rebuilds after compaction, and retain logical backup/restore coverage.
- [PVC corruption or filesystem semantics vary] → Use atomic embedded transactions, exclusive locking, checksums, tested backup/restore, and clearly limit the backend to one writer.
- [Forwarding credentials or primary discovery can expose an internal attack path] → Use a separate internal listener, mutual authentication or scoped shared credentials, fenced epochs, and network policy guidance.
- [The v1 scope is large for a new project] → Build vertical increments in dependency order and define release gates per format/backend rather than weakening the final v1 contract.

## Migration Plan

1. Bootstrap a PVC-backed Riquet instance and verify the selected pinned Confluent contract suite.
2. Export existing Confluent registry state through its supported APIs into Riquet's logical snapshot format.
3. Restore into Riquet under `IMPORT` mode so global IDs, versions, references, configuration, and modes are preserved.
4. Run read-only comparison and real-client smoke tests against the migrated registry.
5. Place Riquet behind the target registry endpoint and move to `READWRITE` when validation succeeds.
6. Retain the source registry read-only for the rollback window. Rollback switches the endpoint back; writes made only to Riquet after cutover require a reverse export/import or must be prevented during a staged cutover.

For Riquet software upgrades, take or verify a logical backup, roll Kafka-backed replicas one at a time, and reject incompatible persistent formats before mutation. PVC deployments stop the single writer, back up, replace the binary, and restart. Any irreversible storage migration requires a new proposal with explicit downgrade behavior.

## Open Questions

- Which exact Confluent Platform release line should be the initial pinned v1 oracle? This can be selected when the reference environment is first automated without changing the architecture or behavioral scope.
- Which Kafka-compatible broker implementations and minimum versions should appear in the first published HA support matrix? The storage contract remains the same; certification can expand incrementally.
