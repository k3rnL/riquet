# Incident response runbook

## Replica not ready or replay lag rising

Compare the replica's applied and committed positions and inspect backend and
HTTP error metrics. Keep lagging replicas out of readiness; do not bypass the
lag gate. Verify Kafka connectivity, TLS/SASL credentials, the state topic's
single partition and compaction policy, and broker quorum. A restarted replica
must replay to the configured lag bound before serving ready traffic.

## Primary loss or write failures

Confirm a new primary and epoch appear in metrics. Clients may retry failed
mutations with the same operation ID; Riquet's committed retry result makes this
idempotent. If an obsolete replica still claims authority, remove it from
traffic and preserve logs—the broker transaction fence, not operator judgment,
decides which producer may commit. Do not delete or recreate the state topic.

## PVC failure

PVC mode has one active writer. Stop replacement loops, retain the volume, and
validate the latest logical backup. If the file is unavailable or corrupt,
restore only into an empty PVC. Never start two writers or copy a live bbolt
file; use the logical backup workflow.

## Suspected bad deployment or credentials

Rollback the chart or binary without changing storage. Riquet 1.x storage and
logical format 1 remain readable across 1.x. Kubernetes secrets hold public,
administrative, internal-forwarding, and Kafka credentials; rotate the affected
secret, restart replicas progressively, and verify readiness. Diagnostics and
request logs intentionally omit tokens, passwords, schema bodies, and backend
connection settings.

## Recovery validation

Before reopening writes, validate health/readiness, compare a sample of IDs and
subject versions, run an unchanged client smoke test, register with a stable
operation ID, and confirm every ready replica converges. Capture a fresh logical
backup after recovery.
