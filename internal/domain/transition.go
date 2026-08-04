package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// EnvelopeVersion is the only transition encoding accepted by this release.
const EnvelopeVersion = 1

// TransitionKind identifies a versioned domain transition payload.
type TransitionKind string

const (
	// TransitionSchemaRegistered adds a schema and subject version.
	TransitionSchemaRegistered TransitionKind = "schema_registered"
	// TransitionVersionDeleted soft- or permanently deletes one version.
	TransitionVersionDeleted TransitionKind = "version_deleted"
	// TransitionSubjectDeleted soft- or permanently deletes a subject.
	TransitionSubjectDeleted TransitionKind = "subject_deleted"
	// TransitionCompatibilitySet sets compatibility at a scope.
	TransitionCompatibilitySet TransitionKind = "compatibility_set"
	// TransitionCompatibilityDel removes subject compatibility.
	TransitionCompatibilityDel TransitionKind = "compatibility_deleted"
	// TransitionModeSet sets registry mode at a scope.
	TransitionModeSet TransitionKind = "mode_set"
	// TransitionModeDeleted removes a subject mode.
	TransitionModeDeleted TransitionKind = "mode_deleted"
)

// Envelope is the durable checksum-protected transition record.
type Envelope struct {
	Version     int             `json:"version"`
	Sequence    Sequence        `json:"sequence"`
	OperationID OperationID     `json:"operationId,omitempty"`
	Kind        TransitionKind  `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	Result      Result          `json:"result"`
	Checksum    string          `json:"checksum"`
}

type registrationTransition struct {
	Schema  Schema         `json:"schema"`
	Version SubjectVersion `json:"subjectVersion"`
}

type deleteVersionTransition struct {
	Subject   string  `json:"subject"`
	Version   Version `json:"version"`
	Permanent bool    `json:"permanent"`
}

type deleteSubjectTransition struct {
	Subject   string    `json:"subject"`
	Versions  []Version `json:"versions"`
	Permanent bool      `json:"permanent"`
}

type compatibilityTransition struct {
	Scope Scope              `json:"scope"`
	Level CompatibilityLevel `json:"level,omitempty"`
}

type modeTransition struct {
	Scope Scope `json:"scope"`
	Mode  Mode  `json:"mode,omitempty"`
}

func newEnvelope(sequence Sequence, operationID OperationID, kind TransitionKind, payload any, result Result) (Envelope, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode transition payload: %w", err)
	}
	envelope := Envelope{
		Version: EnvelopeVersion, Sequence: sequence, OperationID: operationID,
		Kind: kind, Payload: encoded, Result: result,
	}
	envelope.Checksum = envelope.checksum()
	return envelope, nil
}

// Validate verifies the envelope version, required fields, and checksum.
func (e Envelope) Validate() error {
	if e.Version != EnvelopeVersion {
		return domainError(ErrorCorrupt, "transition", "unsupported envelope version %d", e.Version)
	}
	if e.Sequence == 0 || e.Kind == "" || len(e.Payload) == 0 {
		return domainError(ErrorCorrupt, "transition", "missing sequence, kind, or payload")
	}
	if e.Checksum != e.checksum() {
		return domainError(ErrorCorrupt, "transition", "checksum mismatch")
	}
	return nil
}

func (e Envelope) checksum() string {
	type checksumEnvelope struct {
		Version     int
		Sequence    Sequence
		OperationID OperationID
		Kind        TransitionKind
		Payload     json.RawMessage
		Result      Result
	}
	encoded, _ := json.Marshal(checksumEnvelope{
		Version: e.Version, Sequence: e.Sequence, OperationID: e.OperationID,
		Kind: e.Kind, Payload: e.Payload, Result: e.Result,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
