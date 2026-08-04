# Riquet 1.0 release qualification

The release candidate gate is `test/release/qualify.sh`. It runs formatting,
vet, the exact linked-module license audit, unit and race suites, 20 load/soak
iterations, all Docker-backed differential/client/storage/fault/migration tests,
and Helm lint/schema/snapshot tests. `test/release/acceptance.sh` then proves the
clean-install release journey: reference Confluent migration, unchanged Java,
Go, Python, and .NET clients, runtime Kafka primary failure, logical
backup/restore, and fresh Helm install/upgrade for PVC and Kafka HA profiles.

Security qualification covers optional admin plus public/internal authentication boundaries,
constant-time credential checks, trusted proxies, request/parser resource
limits, diagnostic redaction, secret-backed Helm configuration, and fail-closed
migration. The scratch image runs as UID/GID 65532 with only CA certificates and
the four Riquet binaries. Trivy scans the final image for HIGH and CRITICAL
findings; a finding blocks release.

Release artifacts are source archives, statically linked Linux binaries for
amd64 and arm64, a packaged Helm chart, checksums, and the corresponding
multi-platform GHCR image. Versions follow semantic versioning. Storage formats
remain readable within 1.x; breaking API or storage changes require a new major
version and a documented migration path.

`test/release/package.sh v1.0.0` creates the local artifacts. A `v*` tag runs
the full qualification and acceptance gates before GitHub Actions publishes the
release and signed-provenance/SBOM-enabled image; a failed gate publishes
nothing.

The compatibility baseline, endpoint matrix, intentional exception, certified
clients, and certified broker are published in `docs/compatibility.md` and the
machine-readable manifest. Deployment, security, migration/rollback,
performance, upgrade, backup/restore, and incident response procedures live in
`docs/operations`.
