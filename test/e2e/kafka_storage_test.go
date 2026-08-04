//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	logicalbackup "github.com/k3rnL/riquet/internal/backup"
	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/storage"
	kafkastore "github.com/k3rnL/riquet/internal/storage/kafka"
	"github.com/k3rnL/riquet/internal/storage/storagetest"
)

func TestKafkaStorageConformance(t *testing.T) {
	broker := provisionKafka(t)
	var serial atomic.Uint64
	storagetest.Run(t, func(t testing.TB) storage.Store {
		t.Helper()
		topic := fmt.Sprintf("riquet-conformance-%d-%d", time.Now().UnixNano(), serial.Add(1))
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		store, err := kafkastore.Open(ctx, kafkastore.Options{
			Brokers: []string{broker}, Topic: topic, AutoCreateTopic: true,
			TransactionalID: "riquet-conformance-txn-" + topic,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestKafkaPrimaryElectionAndFailover(t *testing.T) {
	broker := provisionKafka(t)
	topic := fmt.Sprintf("riquet-election-%d", time.Now().UnixNano())
	transactionalID := "riquet-election-txn-" + topic
	open := func(t *testing.T) *kafkastore.Store {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		store, err := kafkastore.Open(ctx, kafkastore.Options{
			Brokers: []string{broker}, Topic: topic, AutoCreateTopic: true,
			TransactionalID: transactionalID, RequireLeadership: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	}
	firstStore := open(t)
	first, err := kafkastore.NewCoordinator(firstStore, kafkastore.CoordinatorOptions{Advertisement: "https://replica-1.internal"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	firstLease, err := first.Acquire(ctx, "replica-1")
	if err != nil {
		t.Fatal(err)
	}
	if firstLease.Epoch != 1 {
		t.Fatalf("first epoch = %d, want 1", firstLease.Epoch)
	}
	if err := firstStore.Commit(ctx, kafkaEnvelope(t, 1)); err != nil {
		t.Fatal(err)
	}

	secondStore := open(t)
	followerState := domain.NewState()
	if err := secondStore.Replay(ctx, 0, func(envelope domain.Envelope) error {
		var applyErr error
		followerState, applyErr = followerState.Apply(envelope)
		return applyErr
	}); err != nil {
		t.Fatal(err)
	}
	followerMachine := domain.NewMachine(followerState, nil, nil)
	leaderMachine := domain.NewMachine(followerState, secondStore, nil)
	followCtx, stopFollow := context.WithCancel(context.Background())
	defer stopFollow()
	go func() {
		_ = secondStore.Follow(followCtx, followerMachine.State().Sequence(), followerMachine.ApplyCommitted)
	}()
	second, err := kafkastore.NewCoordinator(secondStore, kafkastore.CoordinatorOptions{Advertisement: "https://replica-2.internal"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Release(context.Background(), storage.Lease{}) })
	type acquisition struct {
		lease storage.Lease
		err   error
	}
	acquired := make(chan acquisition, 1)
	go func() {
		lease, err := second.Acquire(ctx, "replica-2")
		acquired <- acquisition{lease: lease, err: err}
	}()
	time.Sleep(250 * time.Millisecond)
	if err := first.Release(ctx, firstLease); err != nil {
		t.Fatal(err)
	}
	result := <-acquired
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.lease.Epoch != firstLease.Epoch+1 {
		t.Fatalf("failover epoch = %d, want %d", result.lease.Epoch, firstLease.Epoch+1)
	}
	if err := firstStore.Commit(ctx, kafkaEnvelope(t, 2)); !errors.Is(err, storage.ErrNotPrimary) {
		t.Fatalf("former primary commit error = %v", err)
	}
	if _, err := leaderMachine.Register(ctx, domain.RegisterCommand{
		OperationID: "operation-2", Subject: "subject", Identity: "identity-2",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`,
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for followerMachine.State().Sequence() != 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if followerMachine.State().Sequence() != 2 {
		t.Fatalf("local follower view sequence = %d, want 2", followerMachine.State().Sequence())
	}
	if err := firstStore.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}
	if health := firstStore.Health(ctx); health.LastSequence != 2 {
		t.Fatalf("follower last sequence = %d, want 2", health.LastSequence)
	}
	lease, address, ok := firstStore.Primary()
	if !ok || lease != result.lease || address != "https://replica-2.internal" {
		t.Fatalf("primary advertisement = %+v, %q, %t", lease, address, ok)
	}
	status := secondStore.Status()
	if !status.Ready || status.Role != "primary" || status.Epoch != result.lease.Epoch || status.Lag != 0 {
		t.Fatalf("primary status = %+v", status)
	}
	var metrics bytes.Buffer
	if err := secondStore.WriteMetrics(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, metric := range []string{"riquet_kafka_primary 1", "riquet_kafka_epoch 2", "riquet_kafka_replay_lag 0"} {
		if !strings.Contains(metrics.String(), metric) {
			t.Fatalf("metrics missing %q:\n%s", metric, metrics.String())
		}
	}
}

func TestKafkaStableProducerFencesObsoleteWriter(t *testing.T) {
	broker := provisionKafka(t)
	topic := fmt.Sprintf("riquet-fencing-%d", time.Now().UnixNano())
	transactionalID := "riquet-fencing-txn-" + topic
	open := func(t *testing.T) *kafkastore.Store {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		store, err := kafkastore.Open(ctx, kafkastore.Options{
			Brokers: []string{broker}, Topic: topic, AutoCreateTopic: true,
			TransactionalID: transactionalID, OperationTimeout: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first := open(t)
	if err := first.Commit(ctx, kafkaEnvelope(t, 1)); err != nil {
		t.Fatal(err)
	}
	second := open(t)
	if err := second.Commit(ctx, kafkaEnvelope(t, 2)); err != nil {
		t.Fatal(err)
	}
	if err := first.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(ctx, kafkaEnvelope(t, 3)); err == nil {
		t.Fatal("obsolete transactional producer was not fenced")
	}
	if err := second.Commit(ctx, kafkaEnvelope(t, 3)); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaCompactionRestartInterruptionAndAmbiguousCommit(t *testing.T) {
	fixture := provisionKafkaFixture(t)
	if !fixture.managed {
		t.Skip("fault test requires the managed Docker Kafka fixture")
	}
	topic := fmt.Sprintf("riquet-faults-%d", time.Now().UnixNano())
	transactionalID := "riquet-faults-txn-" + topic
	open := func(t *testing.T, operationTimeout time.Duration) *kafkastore.Store {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		store, err := kafkastore.Open(ctx, kafkastore.Options{
			Brokers: []string{fixture.broker}, Topic: topic, AutoCreateTopic: true,
			TransactionalID: transactionalID, OperationTimeout: operationTimeout,
		})
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	store := open(t, time.Second)
	state := domain.NewState()
	machine := domain.NewMachine(state, store, nil)
	if _, err := machine.Register(ctx, domain.RegisterCommand{
		OperationID: "fault-1", Subject: "subject", Identity: "fault-1",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`,
	}); err != nil {
		t.Fatal(err)
	}
	firstSnapshot := machine.State().Snapshot()
	if err := store.SaveSnapshot(ctx, firstSnapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Register(ctx, domain.RegisterCommand{
		OperationID: "fault-2", Subject: "subject", Identity: "fault-2",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"int"}`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(ctx, machine.State().Snapshot()); err != nil {
		t.Fatal(err)
	}
	fixture.exec(t, "kafka-configs", "--bootstrap-server", "localhost:9092", "--entity-type", "topics", "--entity-name", topic,
		"--alter", "--add-config", "min.cleanable.dirty.ratio=0.01,segment.ms=100,delete.retention.ms=100")
	time.Sleep(500 * time.Millisecond)

	fixture.restart(t)
	deadline := time.Now().Add(60 * time.Second)
	for !store.Health(ctx).Healthy && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if !store.Health(ctx).Healthy {
		t.Fatalf("store did not recover after broker restart: %+v", store.Health(ctx))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	rebuilt := open(t, 500*time.Millisecond)
	var rebuiltCount int
	if err := rebuilt.Replay(ctx, 0, func(domain.Envelope) error { rebuiltCount++; return nil }); err != nil {
		t.Fatal(err)
	}
	if rebuiltCount != 2 {
		t.Fatalf("compacted/restarted replay count = %d, want 2", rebuiltCount)
	}
	snapshot, err := rebuilt.LoadSnapshot(ctx)
	if err != nil || snapshot == nil || snapshot.Sequence != 2 {
		t.Fatalf("rebuilt snapshot = %+v, %v", snapshot, err)
	}

	// Cancel immediately after Kafka commits but before local observation.
	ambiguousCtx, cancelAmbiguous := context.WithCancel(context.Background())
	rebuilt.SetAfterCommitHookForTest(cancelAmbiguous)
	if err := rebuilt.Commit(ambiguousCtx, kafkaEnvelope(t, 3)); err == nil {
		t.Fatal("lost response boundary unexpectedly acknowledged")
	}
	rebuilt.SetAfterCommitHookForTest(nil)
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer retryCancel()
	if err := rebuilt.Commit(retryCtx, kafkaEnvelope(t, 3)); err != nil {
		t.Fatalf("ambiguous exact retry failed: %v", err)
	}

	// A broker outage must not acknowledge a transition. A replacement
	// producer then fences/aborts any uncertain old transaction and retries.
	fixture.pause(t)
	interruptedCtx, stopInterrupted := context.WithTimeout(context.Background(), 750*time.Millisecond)
	err = rebuilt.Commit(interruptedCtx, kafkaEnvelope(t, 4))
	stopInterrupted()
	if err == nil {
		t.Fatal("network-interrupted mutation was acknowledged")
	}
	fixture.unpause(t)
	_ = rebuilt.Close()
	recovered := open(t, time.Second)
	if err := recovered.Commit(ctx, kafkaEnvelope(t, 4)); err != nil {
		t.Fatalf("retry after network recovery failed: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
}

func TestKafkaLogicalRestoreAndContinue(t *testing.T) {
	fixture := provisionKafkaFixture(t)
	topic := fmt.Sprintf("riquet-restore-%d", time.Now().UnixNano())
	sourceMachine := domain.NewMachine(domain.NewState(), nil, nil)
	if _, err := sourceMachine.Register(context.Background(), domain.RegisterCommand{
		OperationID: "restore-1", Subject: "restored", Identity: "restore-1",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := sourceMachine.SetMode(context.Background(), "restore-mode", domain.Scope{Subject: "restored"}, domain.ModeImport); err != nil {
		t.Fatal(err)
	}
	var portable bytes.Buffer
	if err := logicalbackup.Export(&portable, sourceMachine.State(), "pvc", time.Now()); err != nil {
		t.Fatal(err)
	}
	envelope, err := logicalbackup.Decode(bytes.NewReader(portable.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	options := kafkastore.Options{
		Brokers: []string{fixture.broker}, Topic: topic, AutoCreateTopic: true,
		TransactionalID: "riquet-restore-txn-" + topic,
	}
	store, err := kafkastore.Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := logicalbackup.Restore(ctx, store, envelope); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	reopened, err := kafkastore.Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	snapshot, err := reopened.LoadSnapshot(ctx)
	if err != nil || snapshot == nil || snapshot.Sequence != sourceMachine.State().Sequence() {
		t.Fatalf("Kafka restored snapshot = %+v, %v", snapshot, err)
	}
	state, err := domain.Restore(*snapshot)
	if err != nil {
		t.Fatal(err)
	}
	machine := domain.NewMachine(state, reopened, nil)
	if _, err := machine.Register(ctx, domain.RegisterCommand{
		OperationID: "restore-continue", Subject: "continued", Identity: "restore-continue",
		Type: domain.SchemaTypeAvro, Definition: `{"type":"long"}`,
	}); err != nil {
		t.Fatal(err)
	}
	if machine.State().Sequence() != snapshot.Sequence+1 {
		t.Fatalf("continued sequence = %d", machine.State().Sequence())
	}
}

func kafkaEnvelope(t testing.TB, sequence domain.Sequence) domain.Envelope {
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
	operation := domain.OperationID(fmt.Sprintf("operation-%d", sequence))
	if _, err := machine.Register(context.Background(), domain.RegisterCommand{
		OperationID: operation, Subject: "subject", Identity: string(operation),
		Type: domain.SchemaTypeAvro, Definition: `{"type":"string"}`,
	}); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(envelope)
	if bytes.Contains(encoded, []byte(`"checksum":""`)) {
		t.Fatal("test envelope has no checksum")
	}
	return envelope
}

func provisionKafka(t *testing.T) string {
	return provisionKafkaFixture(t).broker
}

type kafkaFixture struct {
	broker      string
	project     string
	composeFile string
	environment []string
	managed     bool
}

func provisionKafkaFixture(t *testing.T) kafkaFixture {
	t.Helper()
	if brokers := os.Getenv("RIQUET_KAFKA_BROKERS"); brokers != "" {
		return kafkaFixture{broker: brokers}
	}
	port := availablePort(t)
	project := fmt.Sprintf("riquet-kafka-storage-%d", port)
	composeFile, err := filepath.Abs("compose.kafka.yml")
	if err != nil {
		t.Fatal(err)
	}
	environment := append(os.Environ(), fmt.Sprintf("RIQUET_KAFKA_PORT=%d", port))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", "compose", "--project-name", project, "--file", composeFile, "up", "--detach", "--wait")
	command.Env = environment
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("start Kafka fixture: %v: %s", err, output)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		command := exec.CommandContext(cleanupCtx, "docker", "compose", "--project-name", project, "--file", composeFile, "down", "--volumes", "--remove-orphans")
		command.Env = environment
		if output, err := command.CombinedOutput(); err != nil && cleanupCtx.Err() == nil {
			t.Errorf("stop Kafka fixture: %v: %s", err, output)
		}
	})
	return kafkaFixture{
		broker: fmt.Sprintf("127.0.0.1:%d", port), project: project,
		composeFile: composeFile, environment: environment, managed: true,
	}
}

func (f kafkaFixture) command(t testing.TB, timeout time.Duration, arguments ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	base := []string{"compose", "--project-name", f.project, "--file", f.composeFile}
	command := exec.CommandContext(ctx, "docker", append(base, arguments...)...)
	command.Env = f.environment
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Kafka fixture command %v: %v: %s", arguments, err, output)
	}
	return output
}

func (f kafkaFixture) restart(t testing.TB) {
	f.command(t, 2*time.Minute, "restart", "kafka")
	f.command(t, 2*time.Minute, "up", "--detach", "--wait", "kafka")
}

func (f kafkaFixture) pause(t testing.TB) { f.command(t, 30*time.Second, "pause", "kafka") }

func (f kafkaFixture) unpause(t testing.TB) { f.command(t, 30*time.Second, "unpause", "kafka") }

func (f kafkaFixture) exec(t testing.TB, arguments ...string) {
	f.command(t, time.Minute, append([]string{"exec", "--no-TTY", "kafka"}, arguments...)...)
}
