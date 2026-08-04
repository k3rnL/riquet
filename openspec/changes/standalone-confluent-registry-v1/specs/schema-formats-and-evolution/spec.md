## Purpose

Defines how Riquet interprets, identifies, relates, normalizes, validates, and evolves Avro, Protobuf, and JSON Schema definitions.

## ADDED Requirements

### Requirement: Supported schema formats
Riquet SHALL register, retrieve, validate, and evaluate compatibility for Avro, Protobuf, and JSON Schema using Confluent-compatible schema type defaults and representations.

#### Scenario: Schema type is omitted
- **WHEN** a client registers a schema without `schemaType`
- **THEN** Riquet treats the schema as Avro in accordance with the Confluent API contract

#### Scenario: Each supported type is round-tripped
- **WHEN** a valid Avro, Protobuf, or JSON Schema is registered and retrieved
- **THEN** Riquet preserves its externally significant definition, type, and references

### Requirement: Schema validation and identity
Riquet SHALL reject syntactically or semantically invalid schemas and SHALL compute schema equality, identity, and canonical representation with behavior matching the declared Confluent reference version.

#### Scenario: Invalid schema is registered
- **WHEN** a submitted schema cannot be parsed or contains an invalid construct for its declared type
- **THEN** registration fails with the corresponding Confluent-compatible validation error and no ID or version is consumed

#### Scenario: Textually different schemas are semantically identical
- **WHEN** two definitions differ only in ways ignored by the applicable Confluent identity rules
- **THEN** Riquet treats them as the same schema identity for lookup and registration

### Requirement: Schema normalization
Riquet SHALL support request-level and configured normalization for registration and lookup across all supported schema formats.

#### Scenario: Normalized registration is requested
- **WHEN** a client registers semantically equivalent schemas with normalization enabled
- **THEN** Riquet applies format-specific normalization before identity comparison and returns the existing identity where Confluent would do so

### Requirement: Compatibility policies
Riquet SHALL implement `NONE`, `BACKWARD`, `BACKWARD_TRANSITIVE`, `FORWARD`, `FORWARD_TRANSITIVE`, `FULL`, and `FULL_TRANSITIVE` using format-specific evolution rules compatible with the declared Confluent reference version.

#### Scenario: Latest-only compatibility is evaluated
- **WHEN** a non-transitive policy applies to a candidate schema
- **THEN** Riquet compares the candidate with the applicable latest schema and returns the same decision as the reference behavior

#### Scenario: Transitive compatibility is evaluated
- **WHEN** a transitive policy applies to a candidate schema
- **THEN** Riquet compares the candidate with all applicable prior active versions and rejects it if any required comparison fails

#### Scenario: Verbose compatibility is requested
- **WHEN** an incompatible candidate is tested with verbose diagnostics enabled
- **THEN** Riquet returns `is_compatible: false` and actionable format-specific messages in the Confluent response shape

### Requirement: Schema references
Riquet SHALL resolve and validate named references to exact subject versions for Avro, Protobuf, and JSON Schema, and SHALL expose reverse-reference discovery.

#### Scenario: Referenced schema is registered
- **WHEN** every declared reference resolves and the combined schema is valid
- **THEN** Riquet stores the reference associations and uses the resolved graph during parsing and compatibility evaluation

#### Scenario: Referenced version is missing
- **WHEN** any declared reference names a nonexistent, deleted, or otherwise unavailable subject version
- **THEN** Riquet rejects registration atomically with a Confluent-compatible error

#### Scenario: Referenced version is deleted
- **WHEN** a client attempts an operation that would permanently remove a version still referenced by an active schema
- **THEN** Riquet applies the reference-protection behavior of the declared Confluent reference version

### Requirement: Resource limits
Riquet SHALL apply configurable limits to schema size, reference depth, reference count, and parsing work, and SHALL fail boundedly without changing registry state when a limit is exceeded.

#### Scenario: Reference graph exceeds a configured limit
- **WHEN** resolving a schema would exceed the configured reference depth or count
- **THEN** Riquet rejects the request with a stable client-facing error and remains responsive to other requests
