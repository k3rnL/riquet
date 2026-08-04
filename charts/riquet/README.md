# Riquet Helm chart

See the repository [deployment guide](../../docs/operations/deployment.md) for
PVC and Kafka HA installation, security, and availability guarantees.

Validate locally with:

```sh
helm lint charts/riquet
go test ./test/helm -v
```
