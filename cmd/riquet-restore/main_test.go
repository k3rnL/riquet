package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/backup"
	"github.com/k3rnL/riquet/internal/domain"
	boltstore "github.com/k3rnL/riquet/internal/storage/bolt"
)

func TestRunRestoresPVCAndRejectsSecondRestore(t *testing.T) {
	machine := domain.NewMachine(domain.NewState(), nil, nil)
	if _, err := machine.Register(context.Background(), domain.RegisterCommand{
		OperationID: "restore-command", Subject: "subject", Identity: "identity",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`,
	}); err != nil {
		t.Fatal(err)
	}
	var snapshot bytes.Buffer
	if err := backup.Export(&snapshot, machine.State(), "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "restored.db")
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"--data", path}, bytes.NewReader(snapshot.Bytes()), &stdout, &stderr); code != 0 {
		t.Fatalf("run code = %d, stderr = %s", code, stderr.String())
	}
	store, err := boltstore.Open(path, boltstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := store.LoadSnapshot(context.Background())
	_ = store.Close()
	if err != nil || restored == nil || restored.Sequence != machine.State().Sequence() {
		t.Fatalf("restored snapshot = %+v, %v", restored, err)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"--data", path}, bytes.NewReader(snapshot.Bytes()), &stdout, &stderr); code == 0 {
		t.Fatal("second restore unexpectedly succeeded")
	}
	if !strings.Contains(stderr.String(), "not empty") {
		t.Fatalf("second restore error = %s", stderr.String())
	}
}

func TestRunRejectsTamperedSnapshotBeforeCreatingTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "must-not-exist.db")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--data", path}, strings.NewReader(`{"formatVersion":1}`), &stdout, &stderr)
	if code == 0 {
		t.Fatal("invalid snapshot accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("invalid snapshot created target; stat error = %v", err)
	}
}

func TestRunValidateOnlyDoesNotCreateTarget(t *testing.T) {
	var snapshot bytes.Buffer
	if err := backup.Export(&snapshot, domain.NewState(), "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "must-not-exist.db")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--validate-only", "--data", path}, bytes.NewReader(snapshot.Bytes()), &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), "valid sequence=0") {
		t.Fatalf("code = %d, stdout = %s, stderr = %s", code, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("validation created target; stat error = %v", err)
	}
}
