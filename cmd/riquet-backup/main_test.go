package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/k3rnL/riquet/internal/backup"
	"github.com/k3rnL/riquet/internal/domain"
	boltstore "github.com/k3rnL/riquet/internal/storage/bolt"
)

func TestRunCapturesPVC(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	store, err := boltstore.Open(path, boltstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	machine := domain.NewMachine(domain.NewState(), store, nil)
	if _, err := machine.Register(context.Background(), domain.RegisterCommand{
		OperationID: "backup-command", Subject: "subject", Identity: "identity",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"--data", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("run code = %d, stderr = %s", code, stderr.String())
	}
	envelope, err := backup.Decode(bytes.NewReader(stdout.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Snapshot.Sequence != 1 {
		t.Fatalf("captured sequence = %d", envelope.Snapshot.Sequence)
	}
}
