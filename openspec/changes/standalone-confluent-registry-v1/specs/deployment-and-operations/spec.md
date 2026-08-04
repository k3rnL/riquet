## Purpose

Defines how operators configure, deploy, secure, observe, back up, restore, and upgrade Riquet as a standalone service or Kubernetes workload.

## ADDED Requirements

### Requirement: Standalone distribution
Riquet SHALL be distributed as a versioned Go executable and OCI container that can run without a JVM, Kubernetes operator, external database, or Kafka dependency when the PVC backend is selected.

#### Scenario: PVC standalone instance starts
- **WHEN** an operator supplies a writable data path and valid minimal configuration
- **THEN** one Riquet process starts, persists schemas locally, and serves the registry API

### Requirement: Helm deployment
Riquet SHALL provide a versioned Helm chart supporting Kafka-topic and PVC storage profiles, replica configuration, resources, scheduling controls, probes, service exposure, disruption budgets, and secret references.

#### Scenario: Kafka HA profile is installed
- **WHEN** an operator installs the chart with the Kafka backend and multiple replicas
- **THEN** the rendered workload configures shared Kafka registry storage, safe rolling operation, and service routing only to ready replicas

#### Scenario: Invalid PVC HA profile is requested
- **WHEN** chart values select the initial PVC backend with an unsupported active replica count
- **THEN** Helm validation fails with a clear explanation of the backend constraint

### Requirement: Configuration safety
Riquet SHALL support documented file, environment, and flag configuration sources with deterministic precedence, secret-safe diagnostics, and startup validation of invalid or incompatible values.

#### Scenario: Configuration is invalid
- **WHEN** required backend settings are missing or contradictory
- **THEN** startup fails before serving traffic and identifies the invalid setting without printing secret values

### Requirement: Health and observability
Riquet SHALL expose distinct liveness, readiness, and startup checks, Prometheus-compatible metrics, and structured logs containing request correlation and operational state.

#### Scenario: Replica is replaying storage
- **WHEN** a replica has not reached a safe serving position
- **THEN** liveness can remain healthy while startup/readiness reports that traffic must not yet be routed to it

#### Scenario: API request is diagnosed
- **WHEN** an API operation completes or fails
- **THEN** logs and metrics expose its route, result class, latency, and correlation identifier without logging schema bodies or credentials by default

### Requirement: Transport and authentication integration
Riquet SHALL support TLS configuration and deploy safely behind TLS-terminating proxies, and SHALL provide configurable anonymous, HTTP Basic, and bearer-token authentication modes with protected administrative mutations.

#### Scenario: Authentication is required
- **WHEN** a request lacks valid credentials under a configured authenticated mode
- **THEN** Riquet rejects it without revealing protected registry data and emits the configured standards-compatible challenge or bearer response

### Requirement: Backup and restore
Riquet SHALL provide documented, backend-appropriate procedures and tooling to create a consistent backup and restore it without changing schema IDs, subject versions, references, configuration, or modes.

#### Scenario: Registry is restored to an empty installation
- **WHEN** an operator restores a valid backup produced from a compatible Riquet version
- **THEN** the restored registry exposes the same logical state and identifier relationships as the source backup

### Requirement: Upgrade and shutdown safety
Riquet SHALL drain new traffic, finish or safely abandon in-flight mutations, flush durable state, and support documented compatible upgrade and rollback paths.

#### Scenario: Process receives graceful termination
- **WHEN** the runtime sends a termination signal
- **THEN** the instance leaves readiness before exit and does not acknowledge any mutation that is not durably recoverable

#### Scenario: Unsupported storage format is opened
- **WHEN** a binary encounters storage created by an incompatible newer format
- **THEN** it refuses writable startup with an actionable error rather than modifying the store
