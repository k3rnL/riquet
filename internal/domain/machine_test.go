package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"testing/quick"
)

func TestRegisterDeduplicatesIdentityAndVersionsPerSubject(t *testing.T) {
	t.Parallel()

	machine := NewMachine(NewState(), nil, nil)
	first, err := machine.Register(context.Background(), registration("op-1", "alpha", "identity-1"))
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := machine.Register(context.Background(), registration("op-2", "alpha", "identity-1"))
	if err != nil {
		t.Fatal(err)
	}
	secondCommand := registration("op-3", "beta", "identity-1")
	secondCommand.Definition = `{"name":"identity-1","type":"record","fields":[]}`
	secondSubject, err := machine.Register(context.Background(), secondCommand)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, duplicate) {
		t.Fatalf("duplicate result = %+v, want %+v", duplicate, first)
	}
	if secondSubject.SchemaID != first.SchemaID || secondSubject.Version != 1 {
		t.Fatalf("cross-subject result = %+v, want shared ID and version 1", secondSubject)
	}
	if got := machine.State().Sequence(); got != 2 {
		t.Fatalf("sequence = %d, want 2 committed transitions", got)
	}
	stored, ok := machine.State().SchemaByID(first.SchemaID)
	if !ok || stored.Definition != registration("op-1", "alpha", "identity-1").Definition {
		t.Fatalf("cross-subject registration replaced global schema: %+v", stored)
	}
}

func TestCommitFailureDoesNotConsumeIdentityOrVersion(t *testing.T) {
	t.Parallel()

	failing := errors.New("disk unavailable")
	committer := &controlledCommitter{err: failing}
	machine := NewMachine(NewState(), committer, nil)
	if _, err := machine.Register(context.Background(), registration("op-1", "alpha", "identity-1")); CategoryOf(err) != ErrorStorage {
		t.Fatalf("registration error = %v", err)
	}
	if machine.State().Sequence() != 0 || len(machine.State().Subjects(true)) != 0 {
		t.Fatal("failed commit changed visible state")
	}
	committer.err = nil
	result, err := machine.Register(context.Background(), registration("op-1", "alpha", "identity-1"))
	if err != nil {
		t.Fatal(err)
	}
	if result.SchemaID != 1 || result.Version != 1 {
		t.Fatalf("retry consumed identifiers: %+v", result)
	}
}

func TestOperationRetryReturnsCommittedResult(t *testing.T) {
	t.Parallel()

	committer := &controlledCommitter{}
	machine := NewMachine(NewState(), committer, nil)
	command := registration("same-operation", "alpha", "identity-1")
	first, err := machine.Register(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	second, err := machine.Register(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || committer.count != 1 {
		t.Fatalf("retry result/commit count = %+v/%d, want %+v/1", second, committer.count, first)
	}
}

func TestConcurrentRegistrationSerializesVersions(t *testing.T) {
	t.Parallel()

	machine := NewMachine(NewState(), nil, nil)
	const registrations = 32
	results := make(chan Result, registrations)
	errorsCh := make(chan error, registrations)
	var wait sync.WaitGroup
	for index := 0; index < registrations; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			result, err := machine.Register(context.Background(), registration(
				OperationID(fmt.Sprintf("op-%d", index)), "alpha", fmt.Sprintf("identity-%d", index),
			))
			results <- result
			errorsCh <- err
		}(index)
	}
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[Version]bool)
	for result := range results {
		seen[result.Version] = true
	}
	for version := Version(1); version <= registrations; version++ {
		if !seen[version] {
			t.Fatalf("version %d was not allocated", version)
		}
	}
}

func TestCompatibilityConfigurationAndTransitiveInputs(t *testing.T) {
	t.Parallel()

	var previousCounts []int
	checker := func(_ State, _ CompatibilityLevel, _ Schema, previous []Schema) (bool, []string) {
		previousCounts = append(previousCounts, len(previous))
		return true, nil
	}
	machine := NewMachine(NewState(), nil, checker)
	if err := machine.SetCompatibility(context.Background(), "config", Scope{Subject: "alpha"}, CompatibilityBackwardTransitive); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if _, err := machine.Register(context.Background(), registration(
			OperationID(fmt.Sprintf("op-%d", index)), "alpha", fmt.Sprintf("identity-%d", index),
		)); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(previousCounts, []int{1, 2}) {
		t.Fatalf("transitive previous counts = %v", previousCounts)
	}
	if err := machine.DeleteCompatibility(context.Background(), "delete-config", "alpha"); err != nil {
		t.Fatal(err)
	}
	if got := machine.State().EffectiveCompatibility("alpha"); got != CompatibilityBackward {
		t.Fatalf("inherited compatibility = %s", got)
	}
}

func TestModeGatesAndImportPreservesIdentifiers(t *testing.T) {
	t.Parallel()

	machine := NewMachine(NewState(), nil, nil)
	if err := machine.SetMode(context.Background(), "readonly", Scope{}, ModeReadOnly); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Register(context.Background(), registration("blocked", "alpha", "identity-1")); CategoryOf(err) != ErrorReadOnly {
		t.Fatalf("read-only registration error = %v", err)
	}
	if err := machine.SetMode(context.Background(), "import", Scope{}, ModeImport); err != nil {
		t.Fatal(err)
	}
	command := registration("import-schema", "alpha", "identity-1")
	command.RequestedID = 42
	command.RequestedVersion = 9
	result, err := machine.Register(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.SchemaID != 42 || result.Version != 9 {
		t.Fatalf("import result = %+v", result)
	}
	next, err := machine.Register(context.Background(), registration("next", "beta", "identity-2"))
	if err != nil {
		t.Fatal(err)
	}
	if next.SchemaID != 43 {
		t.Fatalf("next ID = %d, want 43", next.SchemaID)
	}
}

func TestReferenceProtectionAndDeleteLifecycle(t *testing.T) {
	t.Parallel()

	machine := NewMachine(NewState(), nil, nil)
	if _, err := machine.Register(context.Background(), registration("base", "base", "base-identity")); err != nil {
		t.Fatal(err)
	}
	dependent := registration("dependent", "dependent", "dependent-identity")
	dependent.References = []Reference{{Name: "base.avsc", Subject: "base", Version: 1}}
	if _, err := machine.Register(context.Background(), dependent); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.DeleteVersion(context.Background(), DeleteVersionCommand{OperationID: "soft", Subject: "base", Version: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.DeleteVersion(context.Background(), DeleteVersionCommand{OperationID: "permanent", Subject: "base", Version: 1, Permanent: true}); CategoryOf(err) != ErrorConflict {
		t.Fatalf("referenced permanent deletion error = %v", err)
	}
	if got := machine.State().Versions("base", false); len(got) != 0 {
		t.Fatalf("soft-deleted version remains active: %v", got)
	}
}

func TestSnapshotRoundTripAndDefensiveCopies(t *testing.T) {
	t.Parallel()

	machine := NewMachine(NewState(), nil, nil)
	if _, err := machine.Register(context.Background(), registration("op", "alpha", "identity")); err != nil {
		t.Fatal(err)
	}
	snapshot := machine.State().Snapshot()
	restored, err := Restore(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := json.Marshal(machine.State().Snapshot())
	right, _ := json.Marshal(restored.Snapshot())
	if string(left) != string(right) {
		t.Fatalf("snapshot round trip differs:\n%s\n%s", left, right)
	}
	snapshot.Subjects["alpha"][0].Deleted = true
	if got := machine.State().Versions("alpha", false); !reflect.DeepEqual(got, []Version{1}) {
		t.Fatalf("external snapshot mutated state: %v", got)
	}
}

func TestReplayPropertyMatchesMachineState(t *testing.T) {
	t.Parallel()

	property := func(values []uint8) bool {
		var envelopes []Envelope
		machine := NewMachine(NewState(), CommitFunc(func(_ context.Context, envelope Envelope) error {
			envelopes = append(envelopes, envelope)
			return nil
		}), nil)
		for index, value := range values {
			identity := fmt.Sprintf("identity-%d", value)
			_, _ = machine.Register(context.Background(), registration(OperationID(fmt.Sprintf("op-%d", index)), "alpha", identity))
		}
		replayed := NewState()
		for _, envelope := range envelopes {
			var err error
			replayed, err = replayed.Apply(envelope)
			if err != nil {
				return false
			}
		}
		left, _ := json.Marshal(machine.State().Snapshot())
		right, _ := json.Marshal(replayed.Snapshot())
		return string(left) == string(right)
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatal(err)
	}
}

func TestFingerprintGUIDMatchesConfluent(t *testing.T) {
	t.Parallel()

	if got := FingerprintGUID(`"string"`, nil); got != "095d71cf-1255-6b9d-5e33-0ad575b3df5d" {
		t.Fatalf("primitive GUID = %q", got)
	}
	withReference := FingerprintGUID(`"example.Common"`, []Reference{{
		Name: "example.Common", Subject: "common", Version: 1,
	}})
	if withReference != "7d9c3148-064e-8899-1975-db468e59ad53" {
		t.Fatalf("referenced GUID = %q", withReference)
	}
}

func TestApplyCommittedAdvancesFollowerWithoutWriting(t *testing.T) {
	t.Parallel()
	var envelope Envelope
	primary := NewMachine(NewState(), CommitFunc(func(_ context.Context, item Envelope) error {
		envelope = item
		return nil
	}), nil)
	if _, err := primary.Register(context.Background(), registration("operation", "alpha", "identity")); err != nil {
		t.Fatal(err)
	}
	writes := 0
	follower := NewMachine(NewState(), CommitFunc(func(context.Context, Envelope) error {
		writes++
		return nil
	}), nil)
	if err := follower.ApplyCommitted(envelope); err != nil {
		t.Fatal(err)
	}
	if err := follower.ApplyCommitted(envelope); err != nil {
		t.Fatal(err)
	}
	if writes != 0 || follower.State().Sequence() != 1 {
		t.Fatalf("follower writes/sequence = %d/%d", writes, follower.State().Sequence())
	}
}

func registration(operationID OperationID, subject, identity string) RegisterCommand {
	return RegisterCommand{
		OperationID: operationID, Subject: subject, Identity: identity,
		Type: SchemaTypeAvro, Definition: fmt.Sprintf(`{"type":"record","name":%q,"fields":[]}`, identity),
	}
}

type controlledCommitter struct {
	mu    sync.Mutex
	err   error
	count int
}

func (c *controlledCommitter) Commit(_ context.Context, _ Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
	return c.err
}
