## Why

Kafka-compatible deployments need a Schema Registry that can be operated independently of a particular Kafka distribution, JVM stack, database service, or Kubernetes operator. Riquet will provide a small Go-based replacement whose compatibility is demonstrated continuously against Confluent Schema Registry rather than asserted from API resemblance alone.

## What Changes

- Introduce a standalone Schema Registry server compatible with the core Confluent Schema Registry v1 REST API and existing Confluent clients.
- Support Avro, Protobuf, and JSON Schema registration, identity, references, normalization, versioning, deletion, and compatibility evaluation.
- Support global and subject-level compatibility configuration plus `READWRITE`, `READONLY`, and migration-oriented `IMPORT` modes.
- Define a storage abstraction with a Kafka-topic backend for multi-replica high availability and a PVC-backed embedded backend for durable single-writer installations.
- Provide deterministic primary coordination, recovery, and read behavior for highly available Kafka-backed deployments.
- Ship a Helm deployment with health probes, metrics, structured logs, secure endpoint configuration, and documented backend-specific availability guarantees.
- Establish automated API, serializer, storage, concurrency, recovery, and differential end-to-end tests that run the same scenarios against Riquet and pinned Confluent reference versions.
- Explicitly defer Schema Linking/exporters, contexts, rule sets, advanced metadata/tags, fine-grained RBAC, field encryption, a web UI, and non-Confluent registry protocols from the v1 compatibility contract.

## Capabilities

### New Capabilities

- `confluent-registry-api`: Confluent-compatible subjects, schemas, versions, configuration, modes, deletion behavior, media types, and errors.
- `schema-formats-and-evolution`: Avro, Protobuf, and JSON Schema parsing, identity, normalization, references, and compatibility rules.
- `pluggable-storage`: A behavioral storage contract with Kafka-topic and PVC-backed implementations and a shared conformance suite.
- `high-availability`: Multi-replica coordination, consistent reads and writes, failover, replay, and recovery for HA-capable storage.
- `compatibility-verification`: Black-box differential tests and real-client interoperability tests against Confluent Schema Registry.
- `deployment-and-operations`: Go service packaging, Helm deployment, configuration, health, observability, security integration, backup, and upgrade behavior.

### Modified Capabilities

None. This is a new project with no existing capability specifications.

## Impact

- Creates the initial Go service, domain model, HTTP API, schema engines, storage implementations, test harnesses, container image, and Helm chart.
- Kafka is required only when the Kafka-topic storage backend is selected; the PVC backend runs without Kafka storage or an external database.
- The public compatibility surface includes HTTP behavior, persistent schema identity and ordering, compatibility decisions, and interoperability with existing Confluent serializers and tooling.
- HA guarantees vary by backend: Kafka-topic storage supports multi-replica service operation, while the initial PVC backend is explicitly single-writer with restart/rescheduling durability.
