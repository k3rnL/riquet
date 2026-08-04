package domain

import (
	"encoding/json"
	"testing"
)

func TestEnvelopeValidationDetectsVersionAndChecksumChanges(t *testing.T) {
	t.Parallel()

	envelope, err := newEnvelope(1, "operation-1", TransitionModeSet, modeTransition{Mode: ModeReadOnly}, Result{})
	if err != nil {
		t.Fatal(err)
	}
	if err := envelope.Validate(); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
	corrupt := envelope
	corrupt.Payload = json.RawMessage(`{"mode":"IMPORT"}`)
	if err := corrupt.Validate(); CategoryOf(err) != ErrorCorrupt {
		t.Fatalf("corrupt envelope error = %v", err)
	}
	unknown := envelope
	unknown.Version++
	unknown.Checksum = unknown.checksum()
	if err := unknown.Validate(); CategoryOf(err) != ErrorCorrupt {
		t.Fatalf("unknown version error = %v", err)
	}
}

func TestApplyRequiresStrictSequence(t *testing.T) {
	t.Parallel()

	envelope, err := newEnvelope(2, "operation-1", TransitionModeSet, modeTransition{Mode: ModeReadOnly}, Result{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewState().Apply(envelope); CategoryOf(err) != ErrorCorrupt {
		t.Fatalf("sequence gap error = %v", err)
	}
}
