// Package storagetest provides the reusable storage behavioral suite.
package storagetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/storage"
)

// Factory creates an isolated empty store and cleanup is registered by caller.
type Factory func(testing.TB) storage.Store

// Run executes the common contract for a storage implementation.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("ordered replay", func(t *testing.T) {
		store := factory(t)
		first := envelope(t, 1, "one")
		second := envelope(t, 2, "two")
		mustCommit(t, store, first)
		mustCommit(t, store, second)
		var got []domain.Sequence
		if err := store.Replay(context.Background(), 0, func(item domain.Envelope) error {
			got = append(got, item.Sequence)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0] != 1 || got[1] != 2 {
			t.Fatalf("replay sequences = %v", got)
		}
	})
	t.Run("sequence gap is atomic", func(t *testing.T) {
		store := factory(t)
		if err := store.Commit(context.Background(), envelope(t, 2, "gap")); err == nil {
			t.Fatal("sequence gap was accepted")
		}
		if got := store.Health(context.Background()).LastSequence; got != 0 {
			t.Fatalf("failed commit changed sequence to %d", got)
		}
	})
	t.Run("idempotent retry", func(t *testing.T) {
		store := factory(t)
		item := envelope(t, 1, "same")
		mustCommit(t, store, item)
		mustCommit(t, store, item)
		if err := store.Commit(context.Background(), envelope(t, 1, "different")); err == nil {
			t.Fatal("conflicting sequence retry was accepted")
		}
	})
	t.Run("replay callback failure", func(t *testing.T) {
		store := factory(t)
		mustCommit(t, store, envelope(t, 1, "one"))
		sentinel := errors.New("stop replay")
		if err := store.Replay(context.Background(), 0, func(domain.Envelope) error { return sentinel }); !errors.Is(err, sentinel) {
			t.Fatalf("Replay error = %v", err)
		}
	})
	t.Run("snapshot and backup", func(t *testing.T) {
		store := factory(t)
		mustCommit(t, store, envelope(t, 1, "one"))
		snapshot := domain.NewState().Snapshot()
		if err := store.SaveSnapshot(context.Background(), snapshot); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.LoadSnapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if loaded == nil || loaded.FormatVersion != snapshot.FormatVersion {
			t.Fatalf("loaded snapshot = %+v", loaded)
		}
		var backup bytes.Buffer
		if err := store.Backup(context.Background(), &backup); err != nil {
			t.Fatal(err)
		}
		if backup.Len() == 0 {
			t.Fatal("backup is empty")
		}
	})
	t.Run("capabilities and health", func(t *testing.T) {
		store := factory(t)
		capabilities := store.Capabilities()
		if !capabilities.Writable || !capabilities.Replay || !capabilities.ConsistentSequence {
			t.Fatalf("required capabilities absent: %+v", capabilities)
		}
		health := store.Health(context.Background())
		if !health.Healthy || health.CheckedAt.IsZero() {
			t.Fatalf("unexpected health: %+v", health)
		}
	})
}

func envelope(t testing.TB, sequence domain.Sequence, operation domain.OperationID) domain.Envelope {
	t.Helper()
	state := domain.NewState()
	var captured domain.Envelope
	machine := domain.NewMachine(state, domain.CommitFunc(func(_ context.Context, item domain.Envelope) error {
		captured = item
		return nil
	}), nil)
	command := domain.RegisterCommand{
		OperationID: operation, Subject: "subject", Identity: string(operation),
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`,
	}
	if sequence > 1 {
		snapshot := state.Snapshot()
		snapshot.Sequence = sequence - 1
		var err error
		state, err = domain.Restore(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		machine = domain.NewMachine(state, domain.CommitFunc(func(_ context.Context, item domain.Envelope) error {
			captured = item
			return nil
		}), nil)
	}
	if _, err := machine.Register(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(captured)
	var copyEnvelope domain.Envelope
	if err := json.Unmarshal(encoded, &copyEnvelope); err != nil {
		t.Fatal(err)
	}
	return copyEnvelope
}

func mustCommit(t testing.TB, store storage.Store, envelope domain.Envelope) {
	t.Helper()
	if err := store.Commit(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
}
