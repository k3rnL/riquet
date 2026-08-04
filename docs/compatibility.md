# Compatibility scope

Riquet 1.0 implements the Confluent Schema Registry v1 REST contract described
by `compatibility/manifest.json`, with Confluent Platform Schema Registry 8.3.0
as the immutable differential oracle. Every supported endpoint in that manifest
links to an executable scenario that issues the same request to Confluent and
Riquet and compares status, headers, response shape, state transition, and
follow-up reads.

## API and schema formats

The supported API includes schema type and ID discovery, subject and version
registration/read/deletion, references and reverse-reference discovery,
compatibility checks, global and subject compatibility configuration, and
global and subject modes. Avro, Protobuf, and JSON Schema registration,
canonical identity, references, validation, and compatibility are supported.

The one intentional response exception is parser diagnostic wording. For a
malformed schema Riquet matches the HTTP status, numeric error code, response
shape, and atomic behavior, but emits a bounded Go parser diagnostic instead of
Confluent's Java exception text.

Confluent enterprise resources are not part of the v1 replacement contract:
RBAC/ACL APIs, Schema Linking/exporters, non-default contexts, schema metadata,
rule sets, tags, aliases, configured normalization, compatibility groups, and
`FORWARD` mode. The migration exporter detects these source features and fails
closed instead of silently dropping them.

## Certified clients

The release suite runs unchanged registry URL integration tests for Confluent
Java SerDes 8.3.0, franz-go Schema Registry client 1.3.0, Confluent Python
client 2.12.2, Confluent .NET client 2.12.0, and Kafka Connect AvroConverter
8.3.0. It also verifies the Confluent wire envelope. Newer client versions are
not claimed until the same tests pass and the manifest is updated.

## Certified Kafka broker

Kafka-backed HA is certified against Confluent Platform Kafka 8.3.0 using the
pinned image recorded in `compatibility/README.md`. The protocol floor is Kafka
2.5 because Riquet requires transactions, read-committed fetches, consumer
groups, and safe producer fencing. Other Confluent-compatible brokers may work,
but are intentionally unlisted until the complete storage and fault suite
passes against an immutable release image.

Compatibility scope changes only through a reviewed manifest update accompanied
by new differential or client scenarios. Scheduled latest-version probes report
drift; they do not silently expand the release claim.
