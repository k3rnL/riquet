#!/bin/sh
set -eu

go test -tags=e2e ./test/e2e -count=1 -timeout=20m -v \
  -run '^(TestConfluentAPIToRiquetMigration|TestLogicalMigrationMatrix|TestKafkaRuntimeForwardingAndFailover|TestJavaConfluentSerializers|TestMaintainedGoSchemaRegistryClientAndWireEnvelope|TestConfluentPythonClientAndWireEnvelope|TestConfluentDotnetClientAndWireEnvelope)$'
test/helm/install-upgrade.sh
