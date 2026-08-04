## Purpose

Defines the automated evidence required to claim Confluent compatibility across API behavior, schema evolution, clients, storage backends, concurrency, and recovery.

## ADDED Requirements

### Requirement: Parameterized black-box contract suite
Riquet SHALL provide an end-to-end suite whose scenarios can target either a Riquet endpoint or an independently started Confluent Schema Registry endpoint without changing scenario logic.

#### Scenario: Contract scenario runs against both servers
- **WHEN** CI executes an in-scope API scenario
- **THEN** the same inputs run against Riquet and the pinned Confluent reference and their normalized observable outcomes are compared

### Requirement: Semantic differential comparison
Differential tests SHALL compare HTTP status, relevant headers, response shape, error codes, compatibility decisions, and resulting observable state while normalizing values such as independently allocated identifiers only when exact equality is not part of the contract.

#### Scenario: Implementations allocate different numeric IDs
- **WHEN** both implementations preserve the same identity relationships but begin from different prior state
- **THEN** comparison maps corresponding IDs rather than reporting a false incompatibility

#### Scenario: An error differs from Confluent
- **WHEN** Riquet returns a different status, numeric error code, response shape, or state transition for an in-scope request
- **THEN** the differential test fails with both normalized request/response traces

### Requirement: Versioned compatibility baseline
The compatibility claim SHALL identify pinned Confluent reference versions, run them in required CI, and test the latest available supported line on a scheduled drift job.

#### Scenario: New Confluent release changes behavior
- **WHEN** the scheduled latest-version suite detects a difference
- **THEN** it records a reviewable compatibility report without silently redefining the pinned release contract

### Requirement: Schema evolution corpus
The suite SHALL contain version-controlled valid, invalid, compatible, and incompatible evolution cases for Avro, Protobuf, and JSON Schema, including references and normalization.

#### Scenario: Compatibility rule is changed
- **WHEN** implementation work changes a format-specific compatibility decision
- **THEN** the complete relevant corpus runs against Riquet and the reference implementation before the change can pass CI

### Requirement: Real client interoperability
The end-to-end suite SHALL verify representative maintained Confluent serializers/deserializers and clients from multiple language ecosystems using Riquet without Riquet-specific client changes.

#### Scenario: Client switches registry URL
- **WHEN** a supported Confluent client is configured with a Riquet URL and otherwise standard settings
- **THEN** it registers, serializes, retrieves, and deserializes supported schemas with the expected Confluent wire representation

### Requirement: Reliability and backend verification
Automated tests SHALL cover concurrent registration, duplicate retries, process termination, primary failover, storage interruption, replay, restart, rolling upgrade, and both initial storage backends.

#### Scenario: Fault is injected during mutation
- **WHEN** the test harness terminates a replica or interrupts storage at a defined mutation boundary
- **THEN** recovery preserves all acknowledged operations, exposes no partial operation, and permits safe retry of an unacknowledged operation

### Requirement: Compatibility exceptions are explicit
Any intentional divergence or version-specific ambiguity SHALL be recorded in a machine-readable compatibility manifest linked to a focused test.

#### Scenario: Confluent versions disagree
- **WHEN** supported reference versions produce different outcomes for the same scenario
- **THEN** Riquet applies its documented version policy and CI reports the exception rather than weakening the comparison globally
