# Riquet Helm chart

See the repository [deployment guide](../../docs/operations/deployment.md) for
PVC and Kafka HA installation, security, and availability guarantees.

Install the released chart from GHCR with:

```sh
helm upgrade --install riquet oci://ghcr.io/k3rnl/charts/riquet \
  --version 1.0.0
```

Validate locally with:

```sh
helm lint charts/riquet
go test ./test/helm -v
```
