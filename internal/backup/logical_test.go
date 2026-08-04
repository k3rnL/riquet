package backup

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/domain"
	boltstore "github.com/k3rnL/riquet/internal/storage/bolt"
)

func TestLogicalExportValidateAndPVCRestore(t *testing.T) {
	state := populatedState(t)
	var encoded bytes.Buffer
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	if err := Export(&encoded, state, "riquet-test", now); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(encoded.String(), "password") || strings.Contains(encoded.String(), "token") {
		t.Fatalf("backup contains credential-like metadata: %s", encoded.String())
	}
	envelope, err := Decode(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	store, err := boltstore.Open(filepath.Join(t.TempDir(), "restored.db"), boltstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if err := Restore(context.Background(), store, envelope); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.LoadSnapshot(context.Background())
	if err != nil || snapshot == nil {
		t.Fatalf("restored snapshot = %+v, %v", snapshot, err)
	}
	restored, err := domain.Restore(*snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Sequence() != state.Sequence() || len(restored.Subjects(true)) != len(state.Subjects(true)) {
		t.Fatalf("restored state differs: %+v", restored.Snapshot())
	}
	if err := Restore(context.Background(), store, envelope); err == nil {
		t.Fatal("non-empty restore target accepted")
	}
}

func TestLogicalBackupRejectsTampering(t *testing.T) {
	var encoded bytes.Buffer
	if err := Export(&encoded, populatedState(t), "riquet-test", time.Now()); err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(encoded.String(), "subject", "tampered", 1)
	if _, err := Decode(strings.NewReader(tampered)); err == nil {
		t.Fatal("tampered backup accepted")
	}
}

func TestCaptureReconstructsSnapshotAndTail(t *testing.T) {
	store, err := boltstore.Open(filepath.Join(t.TempDir(), "source.db"), boltstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	machine := domain.NewMachine(domain.NewState(), store, nil)
	if _, err := machine.Register(context.Background(), domain.RegisterCommand{
		OperationID: "capture-1", Subject: "subject", Identity: "capture-1",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(context.Background(), machine.State().Snapshot()); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Register(context.Background(), domain.RegisterCommand{
		OperationID: "capture-2", Subject: "subject", Identity: "capture-2",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"int"}`,
	}); err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := Capture(context.Background(), store, &encoded, "pvc", time.Now()); err != nil {
		t.Fatal(err)
	}
	envelope, err := Decode(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Snapshot.Sequence != 2 || len(envelope.Snapshot.Subjects["subject"]) != 2 {
		t.Fatalf("captured snapshot = %+v", envelope.Snapshot)
	}
}

func populatedState(t testing.TB) domain.State {
	t.Helper()
	machine := domain.NewMachine(domain.NewState(), nil, nil)
	if _, err := machine.Register(context.Background(), domain.RegisterCommand{
		OperationID: "backup-op", Subject: "subject", Identity: "backup-identity",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := machine.SetCompatibility(context.Background(), "backup-config", domain.Scope{Subject: "subject"}, domain.CompatibilityFull); err != nil {
		t.Fatal(err)
	}
	return machine.State()
}
