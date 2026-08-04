#!/bin/sh
set -eu

make fmt-check
go vet ./...
test/release/license-audit.sh
go test ./...
go test -race ./...
go test ./test/load -count=20 -timeout=2m
go test -tags=e2e ./test/e2e -count=1 -timeout=45m -v
go test ./test/helm -v
