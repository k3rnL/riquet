## 1. Project and Compatibility Foundation

- [x] 1.1 Initialize the Go module, command layout, internal package boundaries, build metadata, linting, unit-test, race-test, and reproducible build commands.
- [x] 1.2 Add CI jobs for formatting, static analysis, unit tests, race tests, generated-file checks, and container builds.
- [x] 1.3 Select and record the initial pinned Confluent Platform oracle version, its image digest, licensing notes, and the required-versus-scheduled version policy.
- [x] 1.4 Define the machine-readable compatibility manifest schema for supported endpoints, reference versions, intentional exceptions, schema formats, clients, and broker certifications.
- [x] 1.5 Implement shared test infrastructure for isolated temporary ports, process/container lifecycle, readiness waiting, artifact capture, and deterministic cleanup.

## 2. Differential Contract Harness

- [x] 2.1 Define a black-box registry target interface covering base URL, authentication, lifecycle, reset/isolation, and trace capture.
- [x] 2.2 Implement target provisioning for the pinned Confluent reference image and a locally built Riquet process/container.
- [x] 2.3 Implement a declarative scenario runner for ordered HTTP requests, captured responses, and assertions on later observable state.
- [x] 2.4 Implement semantic normalization and symbolic ID/version mapping without normalizing fields whose exact values are under test.
- [x] 2.5 Implement differential reporting that emits minimal request, response, state, reference-version, and compatibility-manifest diagnostics.
- [x] 2.6 Add initial discovery scenarios for media types, validation order, duplicate registration, IDs, versions, missing resources, and deletion behavior.
- [x] 2.7 Add required pinned-oracle CI and a non-blocking scheduled latest-release drift workflow.

## 3. Domain State Machine

- [x] 3.1 Define immutable domain types for schemas, identities, subjects, versions, references, configurations, modes, deletions, operation IDs, and logical sequences.
- [x] 3.2 Define stable domain error categories and endpoint-independent result types that preserve information required for Confluent error mapping.
- [x] 3.3 Define the versioned transition envelope, checksums, serialization compatibility tests, and rejection of unknown incompatible payload versions.
- [x] 3.4 Implement immutable materialized-state construction, indexes, snapshots, and deterministic transition replay.
- [x] 3.5 Implement atomic schema registration and lookup decisions, including deduplication, global ID allocation, subject version allocation, and retry idempotency.
- [x] 3.6 Implement subject/version listing, lookup, soft deletion, permanent deletion, and deleted-resource visibility in the domain core.
- [x] 3.7 Implement global/subject compatibility configuration inheritance and removal in the domain core.
- [x] 3.8 Implement global/subject `READWRITE`, `READONLY`, and `IMPORT` mode inheritance, mutation gates, and conflict-safe identifier import.
- [x] 3.9 Add table-driven and property tests for deterministic replay, concurrent command serialization, failed-operation atomicity, and invariant preservation.

## 4. Storage Contract and PVC Backend

- [x] 4.1 Define storage, snapshot, replay, backup, health, capability, and coordination interfaces around the domain transition boundary.
- [x] 4.2 Build the reusable storage conformance suite for commit atomicity, ordering, retry safety, recovery, corruption handling, snapshots, and declared capabilities.
- [x] 4.3 Complete the bbolt persistence spike and record the dependency decision, file format version, transaction model, and performance/recovery findings.
- [x] 4.4 Implement the bbolt/PVC store with atomic transitions, sequence metadata, checksums, snapshots, and exclusive process locking.
- [x] 4.5 Implement PVC startup recovery, incomplete/corrupt-tail handling, clean shutdown, and consistent physical backup.
- [x] 4.6 Run the common storage suite plus restart, forced-termination, lock-contention, and backup/restore tests against the PVC backend.

## 5. Avro Vertical Slice and Core HTTP API

- [x] 5.1 Define the format-engine interface for parsing, validation, identity, normalization, reference resolution, and compatibility comparison.
- [x] 5.2 Select and pin the Avro parsing dependency using focused comparison cases against the Confluent oracle.
- [x] 5.3 Implement Avro validation, Confluent-compatible identity/canonicalization, normalization, references, and bounded parsing limits.
- [x] 5.4 Implement the public HTTP server, middleware chain, graceful lifecycle, request correlation, and Confluent v1 media negotiation.
- [x] 5.5 Implement schema type discovery plus schema registration and lookup by subject/version and global ID.
- [x] 5.6 Implement subject/version listing, reverse-reference discovery, soft deletion, permanent deletion, and deleted-resource query behavior.
- [x] 5.7 Implement global/subject configuration and compatibility-test endpoints, including verbose diagnostics.
- [x] 5.8 Implement global/subject mode endpoints and import-mode request handling.
- [x] 5.9 Implement endpoint-specific request validation and domain-to-Confluent HTTP status, numeric error, and response-body mapping.
- [x] 5.10 Expand the differential suite to cover every implemented endpoint, relevant query parameter, media type, malformed request, and state transition for Avro.

## 6. Schema Evolution Engines

- [x] 6.1 Build a version-controlled Avro evolution corpus covering all seven policies, transitivity, defaults, aliases, unions, enums, fixed types, references, and normalization.
- [x] 6.2 Implement and differential-test Avro backward, forward, full, and transitive compatibility with verbose diagnostics.
- [x] 6.3 Select and pin the Protobuf parser/compiler dependency using import, descriptor, canonicalization, and evolution comparison cases.
- [x] 6.4 Implement Protobuf validation, identity, normalization, imports/references, reverse references, and bounded graph resolution.
- [x] 6.5 Build the Protobuf mutation corpus and implement all compatibility policies with differential diagnostics.
- [x] 6.6 Select and pin the JSON Schema parser/validator dependency and define the supported draft behavior from reference observations.
- [x] 6.7 Implement JSON Schema validation, identity, normalization, external references, reverse references, and bounded graph resolution.
- [x] 6.8 Build the JSON Schema mutation corpus and implement all compatibility policies with differential diagnostics.
- [x] 6.9 Run cross-format identity, missing/deleted reference, cycle, depth, count, size, and resource-exhaustion test suites.

## 7. Kafka Storage and Coordination

- [x] 7.1 Select and pin the Go Kafka client after verifying transactions, producer fencing, custom group coordination, TLS/SASL, and supported broker requirements.
- [x] 7.2 Define and provision the versioned compacted state-topic layout, record keys, retention/compaction requirements, and startup validation.
- [x] 7.3 Implement full-topic replay into the materialized view with committed isolation, high-water tracking, checksums, and readiness gating.
- [x] 7.4 Implement transactional transition commits with stable producer identity, operation idempotency, and acknowledgement only after committed observation.
- [x] 7.5 Implement single-partition primary election, epoch fencing, primary advertisement, and safe recovery before mutation acceptance.
- [x] 7.6 Implement authenticated internal mutation forwarding, loop/epoch protection, retry behavior, timeouts, and Confluent-compatible public errors.
- [x] 7.7 Implement lag-aware follower readiness, backend degradation reporting, role/epoch/position metrics, and local read serving.
- [x] 7.8 Run the common storage suite against Kafka and add compaction rebuild, network interruption, broker restart, stale-primary fencing, and ambiguous-commit tests.
- [x] 7.9 Certify and record the initial Kafka broker/version support matrix using the same automated HA suite.

## 8. HA and Client Interoperability

- [x] 8.1 Build multi-replica end-to-end fixtures supporting controlled replica, network, and Kafka failures at defined mutation boundaries.
- [x] 8.2 Verify concurrent registration and duplicate retries produce stable IDs/versions with no acknowledged loss or partial state.
- [x] 8.3 Verify failover before commit, after commit, and after lost responses, including fencing of a partitioned former primary.
- [x] 8.4 Verify follower convergence, lag-triggered readiness removal, replay from empty local state, and recovery after extended outage.
- [x] 8.5 Verify rolling restart and compatible rolling upgrade behavior while reads remain available and mutation authority transfers safely.
- [x] 8.6 Add Java Confluent serializer/deserializer E2E coverage for Avro, Protobuf, and JSON Schema, including references.
- [x] 8.7 Add representative maintained Go, Python, and .NET Confluent client E2E coverage and assert standard wire-envelope interoperability.
- [x] 8.8 Add Kafka Connect compatibility smoke tests and record any version-specific exceptions in the manifest.

## 9. Configuration, Security, and Observability

- [x] 9.1 Implement typed YAML, environment, and flag configuration with documented precedence, validation, safe defaults, and redacted diagnostics.
- [x] 9.2 Implement separate configurable public, internal, health, and metrics listeners with timeouts and resource limits.
- [x] 9.3 Implement in-process TLS and trusted-proxy configuration while documenting ingress-based TLS termination.
- [x] 9.4 Implement anonymous, constant-time Basic authentication, and validated bearer-token modes without logging credentials.
- [x] 9.5 Define and enforce v1 administrative mutation protection while documenting that Confluent enterprise RBAC is outside the compatibility contract.
- [x] 9.6 Implement liveness, startup, and readiness endpoints reflecting replay, lag, coordination, and backend capability state.
- [x] 9.7 Implement Prometheus metrics and structured logs for API latency/results, role, epoch, replay position, lag, storage health, and graceful shutdown.
- [x] 9.8 Add tests ensuring schemas, tokens, passwords, and backend credentials are not emitted by default logs, errors, metrics, or health payloads.

## 10. Backup, Restore, and Migration

- [x] 10.1 Define and version a backend-neutral logical snapshot containing schemas, IDs, versions, references, deletion state, configurations, and modes.
- [x] 10.2 Implement consistent logical export with validation, checksums, and secret-free metadata for both storage backends.
- [x] 10.3 Implement empty-target restore/import with atomic conflict detection and preservation of all identifiers and relationships.
- [x] 10.4 Implement Confluent API export tooling that produces the Riquet logical snapshot and reports unsupported source features before cutover.
- [x] 10.5 Add PVC-to-Kafka, Kafka-to-PVC, Riquet-to-Riquet, and Confluent-to-Riquet restore tests with post-restore differential verification.
- [x] 10.6 Document staged migration, read-only validation, cutover, rollback limits, backup procedures, and storage-format compatibility policy.

## 11. Packaging and Helm

- [x] 11.1 Create a minimal non-root multi-architecture OCI image with version metadata, signal handling, and vulnerability scanning.
- [x] 11.2 Create the Helm chart with Service, workload, configuration, secrets, resources, scheduling controls, security contexts, and storage profiles.
- [x] 11.3 Add JSON-schema validation that rejects PVC multi-writer configurations and invalid Kafka HA combinations.
- [x] 11.4 Add startup/liveness/readiness probes, Kafka-mode disruption budget, rolling-update settings, and optional ingress/network-policy examples.
- [x] 11.5 Add Helm lint, template snapshot, Kubernetes schema, PVC install/upgrade, and Kafka multi-replica install/upgrade tests.
- [x] 11.6 Document standalone binary, container, PVC Helm, and Kafka HA deployment paths with explicit availability guarantees.

## 12. v1 Release Qualification

- [x] 12.1 Complete the endpoint-by-endpoint compatibility matrix and ensure every in-scope row links to passing differential scenarios.
- [x] 12.2 Run all schema corpora, real-client tests, storage suites, concurrency tests, fault tests, backup/restore tests, and Helm tests in a release candidate pipeline.
- [x] 12.3 Perform load and soak tests for registration, lookup, replay, compaction recovery, follower lag, and primary churn; document tested limits and defaults.
- [x] 12.4 Audit dependency licenses, image contents, authentication boundaries, parser resource limits, and secret handling; resolve release-blocking findings.
- [x] 12.5 Publish API scope, intentional exceptions, Confluent baseline, broker/client support matrices, upgrade policy, and operational runbooks.
- [x] 12.6 Verify a clean installation can migrate a reference registry, switch unchanged Confluent clients to Riquet, survive a primary failure, and restore from backup before tagging v1.0.
