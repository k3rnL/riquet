# Deployment and availability

Riquet is one statically linked Go service. Public API, internal forwarding,
health, and metrics listeners are independently configurable. Configuration
precedence is defaults, strict YAML, `RIQUET_*` environment variables, then
explicit flags.

## Standalone binary

The minimal PVC profile needs only a writable path:

```sh
riquet --listen 0.0.0.0:8081 --data /var/lib/riquet/riquet.db
```

PVC mode guarantees durable acknowledged mutations and restart/rescheduling
recovery. It is deliberately limited to one active writer and does not promise
zero-downtime upgrades. The file lock rejects a second writer.

For separate operational endpoints, use YAML:

```yaml
listeners:
  public: 0.0.0.0:8081
  health: 0.0.0.0:8082
  metrics: 0.0.0.0:9090
storage:
  backend: pvc
  pvc:
    path: /var/lib/riquet/riquet.db
auth:
  mode: anonymous
limits:
  requestBytes: 2097152
  readTimeout: 30s
  writeTimeout: 30s
  shutdownTimeout: 15s
```

Start it with `riquet --config /etc/riquet/config.yaml`. Unknown keys and unsafe
profile combinations fail before listeners open.

## Container

The OCI image is `scratch`-based, runs as UID/GID 65532, has no shell, and
contains the server plus `/riquet-backup`, `/riquet-export`, and
`/riquet-restore`. Mount a writable data directory for PVC mode:

```sh
docker run --rm -p 8081:8081 \
  -v riquet-data:/var/lib/riquet \
  ghcr.io/k3rnl/riquet:1.0.2 \
  --listen :8081 --data /var/lib/riquet/riquet.db
```

Images are built for `linux/amd64` and `linux/arm64`, carry OCI source/version/
revision/date/license labels, and are scanned for fixable high and critical
vulnerabilities in CI.

## Helm: PVC profile

```sh
helm upgrade --install riquet oci://ghcr.io/k3rnl/charts/riquet \
  --version 1.0.2 \
  --set storage.backend=pvc \
  --set storage.pvc.size=5Gi
```

The chart creates a one-replica StatefulSet, ReadWriteOnce claim, public and
headless Services, startup/liveness/readiness probes, non-root restricted
security contexts, resource defaults, and rolling replacement settings. Its
values schema rejects `replicaCount` other than 1 for PVC.

## Helmfile

Attach the values file under the release's `values` key. If it is omitted,
Helm uses the chart's default PVC profile:

```yaml
releases:
  - name: riquet-registry
    chart: oci://ghcr.io/k3rnl/charts/riquet
    version: 1.0.2
    values:
      - ./riquet-values.yaml
```

Confirm the rendered profile before applying it:

```sh
helmfile template | grep RIQUET_STORAGE_BACKEND
```

## Helm: Kafka HA profile

Pre-create a one-partition compacted topic, or explicitly enable chart-driven
creation. Use a replication factor supported by the cluster:

```sh
helm upgrade --install riquet oci://ghcr.io/k3rnl/charts/riquet \
  --version 1.0.2 \
  --set storage.backend=kafka \
  --set replicaCount=3 \
  --set 'storage.kafka.brokers[0]=kafka-0.kafka:9092' \
  --set 'storage.kafka.brokers[1]=kafka-1.kafka:9092' \
  --set 'storage.kafka.brokers[2]=kafka-2.kafka:9092' \
  --set storage.kafka.replicationFactor=3
```

When no internal token Secret is configured, the chart creates
`<release>-riquet-internal` with a random token and preserves that token across
upgrades. For externally managed secrets, create a Secret containing the
`internal-token` key and set `auth.internalTokenSecret.name`. External mode is
recommended for template-only GitOps workflows that require deterministic
secret manifests.

Kafka HA has one fenced mutation authority, but any ready replica accepts
writes and forwards them internally. Reads are local and may lag briefly;
replicas leave readiness when their configured committed-position lag is
exceeded. With Kafka quorum and at least one ready Riquet replica available,
acknowledged state survives primary loss and rolling replacement. The chart
adds a PodDisruptionBudget for this profile.

For TLS/mTLS, put `ca.crt`, `tls.crt`, and `tls.key` in a secret and set:

```yaml
storage:
  kafka:
    tls:
      enabled: true
      secretName: kafka-client-tls
      serverName: kafka.example.internal
```

For SASL, create a secret with `username` and `password`, then choose `plain`,
`scram-sha-256`, or `scram-sha-512`:

```yaml
storage:
  kafka:
    sasl:
      mechanism: scram-sha-512
      secretName: kafka-client-auth
```

## Probes and observability

- `/health/live`: process is running.
- `/health/startup`: backend recovery reached a serveable state.
- `/health/ready`: backend is healthy and Kafka lag is within policy.
- Metrics expose HTTP totals/latency plus role, epoch, applied position,
  committed position, and replay lag.

Terminate with SIGTERM. Riquet stops accepting traffic, shuts down listeners,
finishes or abandons in-flight requests within the configured timeout, and
persists a safe snapshot on the PVC writer or Kafka primary.
