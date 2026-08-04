#!/bin/sh
set -eu

cluster="riquet-helm-${$}"
cleanup() {
  if [ "${RIQUET_KEEP_HELM_CLUSTER:-false}" != "true" ]; then
    kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

kind create cluster --name "$cluster" --wait 90s
docker build --build-arg TARGETARCH=amd64 --build-arg BUILD_DATE=2026-08-04T00:00:00Z -t riquet:helm-test .
kind load docker-image --name "$cluster" riquet:helm-test

helm install pvc charts/riquet \
  --set image.repository=riquet --set image.tag=helm-test --set image.pullPolicy=Never \
  --wait --timeout 2m
kubectl rollout status statefulset/pvc-riquet --timeout=90s
helm upgrade pvc charts/riquet \
  --set image.repository=riquet --set image.tag=helm-test --set image.pullPolicy=Never \
  --set resources.requests.cpu=60m --wait --timeout 2m
kubectl get pod pvc-riquet-0 -o jsonpath='{.status.containerStatuses[0].ready}' | grep true
helm uninstall pvc --wait

helm install kafka charts/riquet \
  --set storage.backend=kafka --set replicaCount=2 \
  --set 'storage.kafka.brokers[0]=unreachable.invalid:9092' \
  --set image.repository=riquet --set image.tag=helm-test --set image.pullPolicy=Never
test "$(kubectl get statefulset kafka-riquet -o jsonpath='{.spec.replicas}')" = "2"
token_before=$(kubectl get secret kafka-riquet-internal -o jsonpath='{.data.internal-token}')
test -n "$token_before"
helm upgrade kafka charts/riquet \
  --set storage.backend=kafka --set replicaCount=3 \
  --set 'storage.kafka.brokers[0]=unreachable.invalid:9092' \
  --set image.repository=riquet --set image.tag=helm-test --set image.pullPolicy=Never
test "$(kubectl get statefulset kafka-riquet -o jsonpath='{.spec.replicas}')" = "3"
token_after=$(kubectl get secret kafka-riquet-internal -o jsonpath='{.data.internal-token}')
test "$token_after" = "$token_before"
helm uninstall kafka
