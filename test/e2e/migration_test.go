//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/backup"
	"github.com/k3rnL/riquet/internal/contract"
	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats/avro"
	jsonschemaformat "github.com/k3rnL/riquet/internal/formats/jsonschema"
	"github.com/k3rnL/riquet/internal/formats/protobuf"
	"github.com/k3rnL/riquet/internal/migration"
	"github.com/k3rnL/riquet/internal/storage"
	boltstore "github.com/k3rnL/riquet/internal/storage/bolt"
	kafkastore "github.com/k3rnL/riquet/internal/storage/kafka"
	"github.com/k3rnL/riquet/internal/transport/confluent"
)

func TestLogicalMigrationMatrix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	sourceState := migrationState(t)

	pvcSource := openBolt(t, "pvc-source.db")
	if err := pvcSource.RestoreSnapshot(ctx, sourceState.Snapshot()); err != nil {
		t.Fatal(err)
	}
	pvcState := loadState(t, ctx, pvcSource)

	broker := provisionKafka(t)
	kafkaTarget, err := kafkastore.Open(ctx, kafkastore.Options{
		Brokers: []string{broker}, Topic: fmt.Sprintf("riquet-migration-%d", time.Now().UnixNano()),
		AutoCreateTopic: true, TransactionalID: fmt.Sprintf("riquet-migration-txn-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kafkaTarget.Close() })
	restoreLogical(t, ctx, kafkaTarget, pvcState, "pvc")
	kafkaState := loadState(t, ctx, kafkaTarget)
	assertPostRestoreDifferential(t, pvcState, kafkaState)

	pvcFromKafka := openBolt(t, "pvc-from-kafka.db")
	restoreLogical(t, ctx, pvcFromKafka, kafkaState, "kafka")
	assertPostRestoreDifferential(t, kafkaState, loadState(t, ctx, pvcFromKafka))

	riquetTarget := openBolt(t, "riquet-to-riquet.db")
	restoreLogical(t, ctx, riquetTarget, pvcState, "riquet")
	assertPostRestoreDifferential(t, pvcState, loadState(t, ctx, riquetTarget))
}

func TestConfluentAPIToRiquetMigration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	port := availablePort(t)
	oracle, err := (contract.ComposeProvisioner{
		Name: "confluent-8.3.0", File: "compose.oracle.yml", Project: fmt.Sprintf("riquet-migration-oracle-%d", port),
		Environment: append(os.Environ(), fmt.Sprintf("RIQUET_ORACLE_PORT=%d", port)),
		BaseURL:     fmt.Sprintf("http://127.0.0.1:%d", port), ArtifactsDir: filepath.Join(t.TempDir(), "artifacts"),
	}).Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTarget(t, oracle) })

	postJSON(t, ctx, oracle.Client(), oracle.BaseURL().String()+"/subjects/migrated/versions", map[string]any{
		"schema": `{"type":"record","name":"Migrated","fields":[{"name":"value","type":"string"}]}`,
	})
	postJSON(t, ctx, oracle.Client(), oracle.BaseURL().String()+"/subjects/retired/versions", map[string]any{
		"schemaType": "JSON", "schema": `{"type":"string"}`,
	})
	putJSON(t, ctx, oracle.Client(), oracle.BaseURL().String()+"/config/migrated", map[string]any{"compatibility": "FULL"})
	deleteRequest(t, ctx, oracle.Client(), oracle.BaseURL().String()+"/subjects/retired/versions/1")

	var snapshot bytes.Buffer
	report, err := migration.ExportConfluent(ctx, &snapshot, migration.ConfluentOptions{BaseURL: oracle.BaseURL().String()})
	if err != nil {
		t.Fatalf("export pinned Confluent oracle: %v (report %+v)", err, report)
	}
	if report.Subjects != 2 || report.Versions != 2 || report.SoftDeletedVersions != 1 {
		t.Fatalf("unexpected Confluent export report: %+v", report)
	}
	envelope, err := backup.Decode(bytes.NewReader(snapshot.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	targetStore := openBolt(t, "confluent-to-riquet.db")
	if err := backup.Restore(ctx, targetStore, envelope); err != nil {
		t.Fatal(err)
	}
	targetState := loadState(t, ctx, targetStore)
	targetServer := newRegistryServer(targetState)
	defer targetServer.Close()
	target, err := contract.NewEndpointTarget("restored-riquet", targetServer.URL, targetServer.Client(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	oracleReadback, err := contract.NewEndpointTarget("confluent-readback", oracle.BaseURL().String(), &http.Client{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	scenario := migrationReadScenario()
	oracleTrace, err := scenario.RunPrepared(ctx, oracleReadback)
	if err != nil {
		t.Fatal(err)
	}
	targetTrace, err := scenario.RunPrepared(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if difference := contract.CompareTraces(oracleTrace, targetTrace, contract.CompareOptions{
		RelevantHeaders: []string{"Content-Type"}, OpaqueFields: map[string]bool{"message": true},
	}); difference != nil {
		t.Fatalf("post-migration differential mismatch at exchange %d %s: %s; oracle=%s riquet=%s",
			difference.Exchange, difference.Path, difference.Message,
			oracleTrace.Exchanges[difference.Exchange].ResponseBody, targetTrace.Exchanges[difference.Exchange].ResponseBody)
	}
}

func migrationState(t testing.TB) domain.State {
	t.Helper()
	machine := domain.NewMachine(domain.NewState(), nil, nil)
	ctx := context.Background()
	if err := machine.SetMode(ctx, "matrix-import", domain.Scope{}, domain.ModeImport); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Register(ctx, domain.RegisterCommand{
		OperationID: "matrix-avro", Subject: "events", Identity: "matrix-avro",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`, RequestedID: 7, RequestedVersion: 2, Timestamp: 1000,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Register(ctx, domain.RegisterCommand{
		OperationID: "matrix-json", Subject: "retired", Identity: "matrix-json",
		Type: domain.SchemaTypeJSON, Definition: `{"type":"integer"}`, RequestedID: 11, RequestedVersion: 1, Timestamp: 2000,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.DeleteVersion(ctx, domain.DeleteVersionCommand{OperationID: "matrix-delete", Subject: "retired", Version: 1}); err != nil {
		t.Fatal(err)
	}
	if err := machine.SetCompatibility(ctx, "matrix-config", domain.Scope{Subject: "events"}, domain.CompatibilityFull); err != nil {
		t.Fatal(err)
	}
	if err := machine.SetMode(ctx, "matrix-mode", domain.Scope{}, domain.ModeReadOnly); err != nil {
		t.Fatal(err)
	}
	return machine.State()
}

func restoreLogical(t testing.TB, ctx context.Context, target storage.Store, state domain.State, source string) {
	t.Helper()
	var encoded bytes.Buffer
	if err := backup.Export(&encoded, state, source, time.Now()); err != nil {
		t.Fatal(err)
	}
	envelope, err := backup.Decode(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if err := backup.Restore(ctx, target, envelope); err != nil {
		t.Fatal(err)
	}
}

func openBolt(t testing.TB, name string) *boltstore.Store {
	t.Helper()
	store, err := boltstore.Open(filepath.Join(t.TempDir(), name), boltstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func loadState(t testing.TB, ctx context.Context, store storage.Store) domain.State {
	t.Helper()
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot == nil {
		t.Fatalf("load restored snapshot: %+v, %v", snapshot, err)
	}
	state, err := domain.Restore(*snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func assertPostRestoreDifferential(t testing.TB, source, target domain.State) {
	t.Helper()
	sourceServer := newRegistryServer(source)
	defer sourceServer.Close()
	targetServer := newRegistryServer(target)
	defer targetServer.Close()
	sourceTarget, _ := contract.NewEndpointTarget("source", sourceServer.URL, sourceServer.Client(), nil, nil)
	targetTarget, _ := contract.NewEndpointTarget("target", targetServer.URL, targetServer.Client(), nil, nil)
	scenario := contract.Scenario{Name: "logical-restore-observables", Steps: []contract.Step{
		{Name: "subjects", Method: http.MethodGet, Path: "/subjects?deleted=true", Expect: contract.Expectation{Status: http.StatusOK}},
		{Name: "events versions", Method: http.MethodGet, Path: "/subjects/events/versions?deleted=true", Expect: contract.Expectation{Status: http.StatusOK}},
		{Name: "event", Method: http.MethodGet, Path: "/subjects/events/versions/2?deleted=true", Expect: contract.Expectation{Status: http.StatusOK}},
		{Name: "retired", Method: http.MethodGet, Path: "/subjects/retired/versions/1?deleted=true", Expect: contract.Expectation{Status: http.StatusOK}},
		{Name: "config", Method: http.MethodGet, Path: "/config/events", Expect: contract.Expectation{Status: http.StatusOK}},
		{Name: "mode", Method: http.MethodGet, Path: "/mode", Expect: contract.Expectation{Status: http.StatusOK}},
	}}
	ctx := context.Background()
	left, err := scenario.RunPrepared(ctx, sourceTarget)
	if err != nil {
		t.Fatal(err)
	}
	right, err := scenario.RunPrepared(ctx, targetTarget)
	if err != nil {
		t.Fatal(err)
	}
	if difference := contract.CompareTraces(left, right, contract.CompareOptions{}); difference != nil {
		t.Fatalf("post-restore differential mismatch at exchange %d %s: %s", difference.Exchange, difference.Path, difference.Message)
	}
}

func newRegistryServer(state domain.State) *httptest.Server {
	machine := domain.NewMachine(state, nil, nil)
	server := confluent.NewServer(machine, avro.Engine{}, protobuf.Engine{}, jsonschemaformat.Engine{})
	return httptest.NewServer(server.Handler())
}

func migrationReadScenario() contract.Scenario {
	return contract.Scenario{Name: "confluent-migration-readback", Steps: []contract.Step{
		{Name: "subjects", Method: http.MethodGet, Path: "/subjects?deleted=true", Expect: contract.Expectation{Status: http.StatusOK}},
		{Name: "migrated versions", Method: http.MethodGet, Path: "/subjects/migrated/versions?deleted=true", Expect: contract.Expectation{Status: http.StatusOK}},
		{Name: "migrated schema", Method: http.MethodGet, Path: "/subjects/migrated/versions/1?deleted=true", Expect: contract.Expectation{Status: http.StatusOK}},
		{Name: "retired schema", Method: http.MethodGet, Path: "/subjects/retired/versions/1?deleted=true", Expect: contract.Expectation{Status: http.StatusOK}},
		{Name: "subject config", Method: http.MethodGet, Path: "/config/migrated?defaultToGlobal=false", Expect: contract.Expectation{Status: http.StatusOK}},
		{Name: "global mode", Method: http.MethodGet, Path: "/mode", Expect: contract.Expectation{Status: http.StatusOK}},
	}}
}

func postJSON(t testing.TB, ctx context.Context, client *http.Client, endpoint string, value any) {
	t.Helper()
	requestJSON(t, ctx, client, http.MethodPost, endpoint, value)
}

func putJSON(t testing.TB, ctx context.Context, client *http.Client, endpoint string, value any) {
	t.Helper()
	requestJSON(t, ctx, client, http.MethodPut, endpoint, value)
}

func requestJSON(t testing.TB, ctx context.Context, client *http.Client, method, endpoint string, value any) {
	t.Helper()
	body, _ := json.Marshal(value)
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, _ := io.ReadAll(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("%s %s: %s: %s", method, endpoint, response.Status, responseBody)
	}
}

func deleteRequest(t testing.TB, ctx context.Context, client *http.Client, endpoint string) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("DELETE %s: %s: %s", endpoint, response.Status, body)
	}
}
