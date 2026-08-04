## Purpose

Defines a backend-independent persistence contract and the initial Kafka-topic and PVC-backed storage modes with explicit durability and concurrency guarantees.

## ADDED Requirements

### Requirement: Atomic registry transitions
The storage contract SHALL atomically persist each accepted registry state transition, including ID allocation, subject version allocation, configuration, mode, reference, and deletion changes.

#### Scenario: Persistence fails during registration
- **WHEN** a backend cannot durably commit a registration transition
- **THEN** no portion of the registration becomes observable and no schema ID or subject version is permanently consumed

#### Scenario: Duplicate operation is retried
- **WHEN** an operation is retried after its result was lost but its transition was committed
- **THEN** the backend permits the service to identify or safely reproduce the committed result without duplicating state

### Requirement: Ordered recovery
Every backend SHALL expose a durable ordering or equivalent consistent snapshot mechanism from which Riquet can reconstruct the complete authoritative registry state.

#### Scenario: Service restarts after committed mutations
- **WHEN** a Riquet process starts against an existing backend
- **THEN** it reconstructs all committed state in order before reporting itself ready

#### Scenario: Incomplete tail is encountered
- **WHEN** recovery encounters an uncommitted, truncated, or otherwise invalid terminal record
- **THEN** Riquet does not expose partially applied state and reports an actionable storage health failure

### Requirement: Kafka-topic storage
The Kafka backend SHALL use a configurable Kafka-compatible cluster as durable registry storage and SHALL support the coordination and replay semantics required for multi-replica operation.

#### Scenario: Kafka-backed state is rebuilt
- **WHEN** a new replica joins with no local materialized state
- **THEN** it replays the configured registry topic to the committed high-water mark before becoming ready

#### Scenario: Kafka storage is unavailable
- **WHEN** the backend cannot confirm durable writes
- **THEN** Riquet rejects or times out mutations without acknowledging uncommitted state and exposes degraded health

### Requirement: PVC-backed storage
The PVC backend SHALL persist registry state in an embedded transactional store on a mounted filesystem and SHALL enforce a single-writer runtime contract.

#### Scenario: PVC-backed instance is rescheduled
- **WHEN** an instance stops and another instance mounts the same intact volume
- **THEN** the replacement reconstructs the last committed registry state and resumes service

#### Scenario: Multiple writers target one PVC store
- **WHEN** more than one live process attempts writable access to the same PVC-backed store
- **THEN** all but one fail safely or remain non-ready rather than risk concurrent corruption

### Requirement: Backend capability reporting
Each backend SHALL declare its support for multi-replica coordination, writable operation, recovery, backup, and consistency features, and startup SHALL reject configurations whose requested service mode exceeds those capabilities.

#### Scenario: HA replica count uses a single-writer backend
- **WHEN** configuration requests active multi-replica service with the initial PVC backend
- **THEN** startup fails with a clear configuration error or the Helm deployment rejects that combination before installation

### Requirement: Storage conformance suite
Every storage implementation SHALL pass the same behavioral suite for atomicity, ordering, recovery, deletion, retry safety, and failure handling, plus backend-specific capability tests.

#### Scenario: New backend is introduced
- **WHEN** a new storage implementation is added
- **THEN** it cannot be accepted as supported until the common conformance suite passes for every capability it declares
