## Purpose

Defines the externally observable Confluent Schema Registry API contract that lets existing clients use Riquet by changing only the registry URL.

## ADDED Requirements

### Requirement: Core registry resources
Riquet SHALL implement the Confluent Schema Registry v1 operations for registering and looking up schemas, listing schema types, managing subjects and versions, fetching schemas by global ID, and discovering referencing schema versions.

#### Scenario: Existing Confluent client registers and reads a schema
- **WHEN** a conforming Confluent client registers a schema and subsequently retrieves it by subject/version and global ID
- **THEN** Riquet returns the same successful status classes, response fields, and schema content expected from the declared Confluent reference version

#### Scenario: Equivalent schema is registered twice
- **WHEN** the same schema identity is registered more than once under the same subject
- **THEN** Riquet returns the existing global ID and subject version without creating another version

### Requirement: Subject versioning and global identity
Riquet SHALL maintain monotonically increasing versions within each subject and registry-wide schema IDs whose reuse and deduplication behavior matches the declared Confluent reference version.

#### Scenario: Shared schema appears under two subjects
- **WHEN** an identical schema is registered under two subjects using the same schema type and identity rules
- **THEN** Riquet applies Confluent-compatible global ID reuse while maintaining independent subject versions

#### Scenario: Concurrent registrations target one subject
- **WHEN** multiple clients concurrently register equivalent or distinct valid schemas under one subject
- **THEN** each accepted identity has one stable ID/version result and subject versions contain no collisions or gaps caused by failed operations

### Requirement: Deletion lifecycle
Riquet SHALL support Confluent-compatible soft deletion and permanent deletion of subject versions and complete subjects, including visibility through deleted-resource query options.

#### Scenario: Subject is soft deleted
- **WHEN** a client deletes a subject without requesting permanent deletion
- **THEN** its active versions disappear from normal listings while remaining available through the applicable deleted-resource operations

#### Scenario: Subject is permanently deleted
- **WHEN** a previously soft-deleted subject is deleted permanently
- **THEN** Riquet removes its recoverable subject-version associations and returns Confluent-compatible results for later lookups

### Requirement: Compatibility configuration
Riquet SHALL expose global and subject-level compatibility configuration with subject inheritance and deletion semantics compatible with Confluent Schema Registry.

#### Scenario: Subject inherits global configuration
- **WHEN** no subject-specific compatibility value exists
- **THEN** compatibility evaluation for that subject uses the current global value

#### Scenario: Subject configuration is removed
- **WHEN** a client deletes an existing subject-level configuration
- **THEN** the subject resumes inheriting the global configuration and the API returns the expected prior or effective value

### Requirement: Registry modes
Riquet SHALL support global and subject-level `READWRITE`, `READONLY`, and `IMPORT` modes, with mutation permissions and inheritance matching the documented Confluent behavior in scope for v1.

#### Scenario: Registry is read-only
- **WHEN** the effective mode is `READONLY` and a client attempts a schema mutation
- **THEN** Riquet rejects the mutation without altering registry state and returns a Confluent-compatible error

#### Scenario: Migration imports preserved identifiers
- **WHEN** an authorized client in `IMPORT` mode submits a valid schema version with explicit migration identifiers
- **THEN** Riquet preserves the supplied compatible ID/version mapping or rejects the request atomically when it conflicts

### Requirement: HTTP compatibility
Riquet SHALL honor Confluent v1 media types, supported JSON media types, relevant query parameters, request validation, response shapes, HTTP statuses, and numeric error codes for all in-scope endpoints.

#### Scenario: Unsupported or invalid request is received
- **WHEN** a request has an invalid schema, unknown subject/version, malformed body, or unsupported parameter value
- **THEN** Riquet returns the endpoint-specific HTTP status and Confluent-style `error_code` and `message` body without partial mutation

#### Scenario: Vendor media type is requested
- **WHEN** a client sends or accepts `application/vnd.schemaregistry.v1+json`
- **THEN** Riquet processes the request and returns a compatible response content type and JSON representation
