# Riquet

Riquet is a standalone, Go-based replacement for the Confluent Schema
Registry. It serves the Confluent v1 REST contract and existing Avro,
Protobuf, and JSON Schema clients without requiring a JVM, database operator,
or vendor-specific Kafka distribution.

Two storage profiles are built into the same binary:

- **PVC:** embedded bbolt storage for one durable writer; Kafka is not needed.
- **Kafka HA:** a compacted Kafka topic, transactional fencing, primary
  election, authenticated mutation forwarding, and locally served reads on
  every caught-up replica.

The compatibility claim is tested continuously against pinned Confluent
Platform 8.3.0 and maintained Java, Go, Python, and .NET clients. See
[`compatibility/manifest.json`](compatibility/manifest.json) for the
machine-readable scope and intentional exceptions.

## Quick start

```sh
go build -o riquet ./cmd/riquet
./riquet --listen :8081 --data ./riquet.db
```

Then register a schema through the standard API:

```sh
curl --fail-with-body \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data '{"schema":"{\"type\":\"string\"}"}' \
  http://127.0.0.1:8081/subjects/example-value/versions
```

Operational guides:

- [Deployment and availability](docs/operations/deployment.md)
- [Backup, restore, and migration](docs/operations/migration.md)
- [Security](docs/operations/security.md)
- [Compatibility scope](docs/compatibility.md)
- [Performance and tested limits](docs/operations/performance.md)
- [Incident response](docs/operations/incident-response.md)

## Development

```sh
make fmt-check
make test
make test-race
go test ./test/helm -v
```

Docker-backed compatibility and HA suites use the `e2e` build tag. The pinned
Confluent images are test dependencies only and are not redistributed.

Riquet is licensed under Apache License 2.0.
