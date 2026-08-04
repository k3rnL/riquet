## Purpose

Defines the externally observable availability, coordination, consistency, failure recovery, and rolling-operation behavior of multi-replica Riquet deployments.

## ADDED Requirements

### Requirement: Coordinated mutation authority
In an HA-capable deployment, Riquet SHALL establish one effective mutation authority for each registry state stream while allowing any healthy replica endpoint to accept client requests.

#### Scenario: Follower receives a mutation
- **WHEN** a healthy non-primary replica receives a valid mutation request
- **THEN** it forwards or coordinates that request with the current primary and returns the authoritative result in the normal API shape

#### Scenario: Two replicas contend for authority
- **WHEN** a partition or election causes multiple replicas to believe they can coordinate mutations
- **THEN** backend fencing ensures that at most one can commit authoritative transitions for the same epoch

### Requirement: Primary failover
The Kafka-backed deployment SHALL elect a replacement primary after loss of the current primary without losing acknowledged state or accepting conflicting IDs and versions.

#### Scenario: Primary stops after acknowledging a write
- **WHEN** the primary becomes unavailable after returning a successful mutation response
- **THEN** the replacement primary recovers that mutation before accepting dependent writes

#### Scenario: Primary stops before commit
- **WHEN** the primary becomes unavailable before a mutation is durably committed
- **THEN** the mutation is not exposed as successful and a retry produces one valid authoritative outcome

### Requirement: Replica read behavior
Ready replicas SHALL serve reads from a complete committed materialized state, SHALL converge on newly committed mutations, and SHALL leave readiness when their lag exceeds a configured safety threshold.

#### Scenario: Replica catches up after a write
- **WHEN** a mutation is committed while another replica is healthy
- **THEN** the replica applies the ordered transition and exposes the new state after bounded backend propagation

#### Scenario: Replica is excessively stale
- **WHEN** a replica cannot follow committed storage state within the configured threshold
- **THEN** it reports not-ready so normal service routing stops sending it client traffic

### Requirement: Rolling operation
An HA-capable Riquet deployment SHALL support rolling restarts and compatible rolling upgrades while maintaining at least one ready endpoint when backend quorum and capacity remain available.

#### Scenario: Replicas restart one at a time
- **WHEN** replicas are terminated and replaced sequentially under a disruption budget
- **THEN** registry reads remain available and mutation authority transfers without loss of acknowledged state

### Requirement: Availability observability
Riquet SHALL expose the current role, coordination epoch, applied storage position, backend health, and replay lag through machine-readable metrics and health diagnostics without exposing credentials.

#### Scenario: Operator investigates a lagging replica
- **WHEN** a replica falls behind authoritative storage
- **THEN** its health diagnostics and metrics identify the backend state and lag needed to distinguish replay, connectivity, and leadership problems
