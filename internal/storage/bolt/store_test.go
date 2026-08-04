package bolt

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/storage"
	"github.com/k3rnL/riquet/internal/storage/storagetest"
	bolt "go.etcd.io/bbolt"
)

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t testing.TB) storage.Store {
		t.Helper()
		store, err := Open(filepath.Join(t.TempDir(), "riquet.db"), Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRestartAndPhysicalBackup(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "riquet.db")
	store := openTestStore(t, path)
	envelope := testEnvelope(t)
	if err := store.Commit(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	var backup bytes.Buffer
	if err := store.Backup(context.Background(), &backup); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTestStore(t, path)
	assertOneTransition(t, reopened)

	backupPath := filepath.Join(t.TempDir(), "backup.db")
	if err := os.WriteFile(backupPath, backup.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	restored := openTestStore(t, backupPath)
	assertOneTransition(t, restored)
}

func TestExclusiveOpenFailsBoundedly(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "riquet.db")
	first := openTestStore(t, path)
	started := time.Now()
	second, err := Open(path, Options{LockTimeout: 25 * time.Millisecond})
	if second != nil {
		_ = second.Close()
		t.Fatal("second writer acquired the database")
	}
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("exclusive open error/duration = %v/%s", err, time.Since(started))
	}
	if !first.Health(context.Background()).Healthy {
		t.Fatal("failed competing open affected first writer")
	}
}

func TestCorruptTransitionFailsReplay(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "riquet.db")
	store := openTestStore(t, path)
	if err := store.Commit(context.Background(), testEnvelope(t)); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(transitionsBucket).Put(uint64Key(1), []byte(`{"broken":true}`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Replay(context.Background(), 0, func(domain.Envelope) error { return nil }); err == nil {
		t.Fatal("corrupt transition replay succeeded")
	}
}

func TestCloseIsIdempotentAndHealthChanges(t *testing.T) {
	t.Parallel()

	store := openTestStore(t, filepath.Join(t.TempDir(), "riquet.db"))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	health := store.Health(context.Background())
	if health.Healthy || health.Writable || health.Detail != "closed" {
		t.Fatalf("closed health = %+v", health)
	}
}

func TestForcedTerminationRecovery(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "riquet.db")
	marker := filepath.Join(t.TempDir(), "committed")
	command := exec.Command(os.Args[0], "-test.run=TestBoltCrashHelper")
	command.Env = append(os.Environ(), "RIQUET_BOLT_CRASH_PATH="+path, "RIQUET_BOLT_CRASH_MARKER="+marker)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatal("child did not commit before deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("forced process termination unexpectedly succeeded")
	}
	reopened := openTestStore(t, path)
	assertOneTransition(t, reopened)
}

func TestBoltCrashHelper(t *testing.T) {
	path := os.Getenv("RIQUET_BOLT_CRASH_PATH")
	if path == "" {
		return
	}
	store, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(context.Background(), testEnvelope(t)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("RIQUET_BOLT_CRASH_MARKER"), []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {}
}

func BenchmarkCommitDurable(b *testing.B) {
	benchmarkCommit(b, Options{})
}

func BenchmarkCommitNoSync(b *testing.B) {
	benchmarkCommit(b, Options{NoSync: true})
}

func benchmarkCommit(b *testing.B, options Options) {
	store, err := Open(filepath.Join(b.TempDir(), "riquet.db"), options)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		envelope := testEnvelopeSequence(b, domain.Sequence(index+1))
		if err := store.Commit(context.Background(), envelope); err != nil {
			b.Fatal(err)
		}
	}
}

func openTestStore(t testing.TB, path string) *Store {
	t.Helper()
	store, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func testEnvelope(t testing.TB) domain.Envelope { return testEnvelopeSequence(t, 1) }

func testEnvelopeSequence(t testing.TB, sequence domain.Sequence) domain.Envelope {
	t.Helper()
	snapshot := domain.NewState().Snapshot()
	snapshot.Sequence = sequence - 1
	state, err := domain.Restore(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var envelope domain.Envelope
	machine := domain.NewMachine(state, domain.CommitFunc(func(_ context.Context, item domain.Envelope) error {
		envelope = item
		return nil
	}), nil)
	_, err = machine.Register(context.Background(), domain.RegisterCommand{
		OperationID: domain.OperationID("operation"), Subject: "subject", Identity: "identity",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(envelope)
	if bytes.Contains(encoded, []byte(`"checksum":""`)) {
		t.Fatal("test envelope has no checksum")
	}
	return envelope
}

func assertOneTransition(t testing.TB, store *Store) {
	t.Helper()
	count := 0
	if err := store.Replay(context.Background(), 0, func(domain.Envelope) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replayed %d transitions, want 1", count)
	}
}
