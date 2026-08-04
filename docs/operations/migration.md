# Backup, restore, and migration

Riquet logical backup format 1 preserves schemas, global IDs, subject versions,
references, soft-deletion state, compatibility configuration, modes, retry
results, the next ID, and the applied sequence. The envelope has a SHA-256
checksum and contains no credentials. Restore validates the complete envelope
before opening a target and initializes only an empty PVC database or empty
Kafka state topic.

Build the operational tools with the same source revision as the server:

```sh
go build -o bin/riquet-backup ./cmd/riquet-backup
go build -o bin/riquet-export ./cmd/riquet-export
go build -o bin/riquet-restore ./cmd/riquet-restore
```

## Back up Riquet

PVC uses an exclusive file lock. Stop the single Riquet writer cleanly, then
capture its snapshot plus any transition tail:

```sh
bin/riquet-backup \
  --backend pvc \
  --data /var/lib/riquet/riquet.db \
  --output riquet-$(date -u +%Y%m%dT%H%M%SZ).json
```

Kafka capture reads a consistent committed prefix and can run while replicas
are online. It never writes registry state and uses an isolated transactional
client identity:

```sh
RIQUET_KAFKA_BROKERS=kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  bin/riquet-backup \
  --backend kafka \
  --topic _riquet_state \
  --output riquet-$(date -u +%Y%m%dT%H%M%SZ).json
```

Validate metadata, the domain invariants, and checksum without creating a
target:

```sh
bin/riquet-restore --snapshot riquet-backup.json --validate-only
```

Keep logical backups encrypted at rest even though Riquet excludes configured
credentials; schemas can contain commercially sensitive structure.

## Restore into an empty installation

For PVC, the database path may be absent or contain a newly initialized empty
store. It must not contain a snapshot or transition:

```sh
bin/riquet-restore \
  --snapshot riquet-backup.json \
  --backend pvc \
  --data /var/lib/riquet/riquet.db
```

For Kafka, pre-create `_riquet_state` with one partition and compaction, or let
the tool create it explicitly. The topic must be empty:

```sh
RIQUET_KAFKA_BROKERS=kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  bin/riquet-restore \
  --snapshot riquet-backup.json \
  --backend kafka \
  --topic _riquet_state \
  --replication-factor 3 \
  --auto-create-topic
```

Never merge a logical restore into a non-empty target. The restore command
fails without modifying a non-empty backend.

## Staged Confluent-to-Riquet migration

The automated release suite exercises this process against the pinned
Confluent Platform 8.3.0 oracle.

1. Inventory deferred v1 features before scheduling cutover. Riquet v1 cannot
   preserve non-default contexts, Schema Linking/exporters, schema metadata,
   rule sets, schema tags, subject aliases, configured normalization,
   compatibility groups, or `FORWARD` mode.

2. Freeze source writes. With Confluent `mode.mutability=true`, set the global
   mode to `READONLY`:

   ```sh
   curl --fail-with-body \
     -X PUT \
     -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
     --data '{"mode":"READONLY"}' \
     "$SOURCE_REGISTRY/mode"
   ```

3. Export through supported Confluent REST APIs. Credentials are supplied by
   environment variables so they do not appear in the process arguments:

   ```sh
   export RIQUET_EXPORT_BASIC_USERNAME='migration-reader'
   export RIQUET_EXPORT_BASIC_PASSWORD='replace-me'
   bin/riquet-export \
     --source "$SOURCE_REGISTRY" \
     --output confluent-export.json \
     2>confluent-export-report.json
   bin/riquet-restore --snapshot confluent-export.json --validate-only
   ```

   Use `RIQUET_EXPORT_BEARER_TOKEN` instead of the Basic variables for bearer
   authentication. Export is fail-closed: the output file is not replaced when
   unsupported source features, invalid schemas, unresolved references, or a
   changing subject/version inventory is detected. Permanently deleted
   associations are not observable through the Confluent API and cannot be
   exported; the report always records that limit.

4. Restore into an empty target using one of the commands above. Because the
   source snapshot is `READONLY`, the restored Riquet registry remains
   read-only during validation and preserves explicit subject modes.

5. Compare both endpoints before cutover. At minimum compare `/subjects` with
   `deleted=true`, every subject's versions with `deleted=true`, every version,
   `/schemas/ids/{id}`, `/config`, subject overrides using
   `defaultToGlobal=false`, `/mode`, and subject mode overrides. Run supported
   Java, Go, Python, and .NET serializer/deserializer smoke tests against Riquet
   by changing only the registry URL.

6. Quiesce registry clients, repeat the export/restore/verification if any
   source write was possible, change the Riquet global mode to `READWRITE`, and
   switch the registry endpoint. Monitor readiness, API errors, Kafka replay
   lag, and serializer failures before resuming producers.

## Rollback boundary

Keep the source registry read-only and retain its Kafka storage throughout the
rollback window. Switching the endpoint back is lossless only while Riquet has
accepted no unique writes. Once Riquet accepts writes, rollback requires a
reverse Riquet logical backup/import and another verification window, or an
explicit decision to discard those writes. Do not run both registries writable
behind the same endpoint.

## Storage-format compatibility policy

Logical envelope format 1 and domain snapshot format 1 are readable by every
Riquet 1.x release. The bbolt store and Kafka record/checkpoint formats also
remain readable throughout 1.x. A binary must refuse writable startup when it
encounters a newer unknown format; it must never downgrade or partially replay
that store.

Before every upgrade, validate a current logical backup. Roll Kafka replicas
one at a time after confirming version compatibility. For PVC, stop the sole
writer, capture and validate a backup, replace the binary, and restart. Any
future irreversible format change requires a separately documented migration,
rollback procedure, and a new logical envelope version.
