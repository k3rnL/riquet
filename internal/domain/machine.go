package domain

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// Committer atomically durably commits one validated transition.
type Committer interface {
	Commit(context.Context, Envelope) error
}

// CommitFunc adapts a function to Committer.
type CommitFunc func(context.Context, Envelope) error

// Commit calls the adapted function.
func (f CommitFunc) Commit(ctx context.Context, envelope Envelope) error { return f(ctx, envelope) }

// CompatibilityCheck is a deterministic schema compatibility callback.
type CompatibilityCheck func(state State, level CompatibilityLevel, candidate Schema, previous []Schema) (bool, []string)

// Machine serializes commands, commits transitions, and publishes immutable state.
type Machine struct {
	mu     sync.RWMutex
	state  State
	commit Committer
	check  CompatibilityCheck
}

// NewMachine creates a machine from restored state. A nil committer is an in-memory commit.
func NewMachine(state State, committer Committer, check CompatibilityCheck) *Machine {
	if committer == nil {
		committer = CommitFunc(func(context.Context, Envelope) error { return nil })
	}
	if check == nil {
		check = func(State, CompatibilityLevel, Schema, []Schema) (bool, []string) { return true, nil }
	}
	return &Machine{state: state, commit: committer, check: check}
}

// State returns an immutable view of the current committed state.
func (m *Machine) State() State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return State{snapshot: cloneSnapshot(m.state.snapshot)}
}

// ApplyCommitted advances a follower from an already durable transition. It
// never calls the committer and safely ignores an envelope the local command
// path applied while a replay observer was waiting for the machine lock.
func (m *Machine) ApplyCommitted(envelope Envelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if envelope.Sequence <= m.state.Sequence() {
		return nil
	}
	next, err := m.state.Apply(envelope)
	if err != nil {
		return err
	}
	m.state = next
	return nil
}

// Register validates, commits, and applies one schema registration.
func (m *Machine) Register(ctx context.Context, command RegisterCommand) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if result, ok := m.state.ResultFor(command.OperationID); command.OperationID != "" && ok {
		return result, nil
	}
	envelope, result, err := m.planRegister(command)
	if err != nil {
		return Result{}, err
	}
	if envelope.Kind == "" {
		return result, nil
	}
	return m.commitApply(ctx, envelope, result)
}

func (m *Machine) planRegister(command RegisterCommand) (Envelope, Result, error) {
	if command.Subject == "" || command.Identity == "" || command.Definition == "" || !validSchemaType(command.Type) {
		return Envelope{}, Result{}, domainError(ErrorInvalid, "schema", "subject, identity, definition, and valid type are required")
	}
	mode := m.state.EffectiveMode(command.Subject)
	if mode == ModeReadOnly {
		return Envelope{}, Result{}, domainError(ErrorReadOnly, command.Subject, "schema registration is disabled")
	}
	if (command.RequestedID != 0 || command.RequestedVersion != 0) && mode != ModeImport {
		return Envelope{}, Result{}, domainError(ErrorConflict, command.Subject, "explicit ID/version requires IMPORT mode")
	}
	for _, reference := range command.References {
		if reference.Name == "" || reference.Subject == "" || reference.Version < 1 {
			return Envelope{}, Result{}, domainError(ErrorInvalid, "reference", "name, subject, and positive version are required")
		}
		if _, _, ok := m.state.Lookup(reference.Subject, reference.Version, false); !ok {
			return Envelope{}, Result{}, domainError(ErrorNotFound, "reference", "%s/%d", reference.Subject, reference.Version)
		}
	}
	if existingID, ok := m.state.snapshot.IdentityToID[command.Identity]; ok {
		for _, item := range m.state.snapshot.Subjects[command.Subject] {
			if item.SchemaID == existingID && !item.Deleted && !item.Permanent {
				existing := m.state.snapshot.Schemas[existingID]
				return Envelope{}, Result{SchemaID: existingID, GUID: existing.GUID, Version: item.Version}, nil
			}
		}
	}

	id := command.RequestedID
	if existingID, ok := m.state.snapshot.IdentityToID[command.Identity]; ok {
		if id != 0 && id != existingID {
			return Envelope{}, Result{}, domainError(ErrorConflict, "schema", "identity already uses ID %d", existingID)
		}
		id = existingID
	}
	if id == 0 {
		id = m.state.snapshot.NextSchemaID
	}
	if existing, ok := m.state.snapshot.Schemas[id]; ok && existing.Identity != command.Identity {
		return Envelope{}, Result{}, domainError(ErrorConflict, "schema", "ID %d already belongs to another identity", id)
	}
	version := command.RequestedVersion
	if version == 0 {
		version = nextVersion(m.state.snapshot.Subjects[command.Subject])
	}
	for _, item := range m.state.snapshot.Subjects[command.Subject] {
		if item.Version == version {
			return Envelope{}, Result{}, domainError(ErrorConflict, command.Subject, "version %d already exists", version)
		}
	}

	candidate := Schema{
		ID: id, GUID: FingerprintGUID(command.Definition, command.References), Identity: command.Identity,
		Type: command.Type, Definition: command.Definition, References: slices.Clone(command.References),
	}
	if existing, ok := m.state.snapshot.Schemas[id]; ok {
		candidate = existing.Clone()
	}
	previous := m.compatibilitySchemas(command.Subject)
	level := m.state.EffectiveCompatibility(command.Subject)
	if mode != ModeImport && level != CompatibilityNone && len(previous) > 0 {
		compatible, messages := m.check(m.state, level, candidate.Clone(), previous)
		if !compatible {
			return Envelope{}, Result{}, domainError(ErrorIncompatible, command.Subject, "%s", fmt.Sprint(messages))
		}
	}
	result := Result{SchemaID: id, GUID: candidate.GUID, Version: version}
	payload := registrationTransition{Schema: candidate, Version: SubjectVersion{
		Subject: command.Subject, Version: version, SchemaID: id,
		Timestamp: command.Timestamp, Sequence: m.state.Sequence() + 1,
	}}
	envelope, err := newEnvelope(m.state.Sequence()+1, command.OperationID, TransitionSchemaRegistered, payload, result)
	return envelope, result, err
}

// DeleteVersion soft- or permanently deletes one subject version.
func (m *Machine) DeleteVersion(ctx context.Context, command DeleteVersionCommand) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if result, ok := m.state.ResultFor(command.OperationID); command.OperationID != "" && ok {
		return result, nil
	}
	if m.state.EffectiveMode(command.Subject) == ModeReadOnly {
		return Result{}, domainError(ErrorReadOnly, command.Subject, "deletion is disabled")
	}
	item, _, ok := m.state.Lookup(command.Subject, command.Version, command.Permanent)
	if !ok {
		return Result{}, domainError(ErrorNotFound, "version", "%s/%d", command.Subject, command.Version)
	}
	if command.Permanent && !item.Deleted {
		return Result{}, domainError(ErrorConflict, "version", "soft delete is required before permanent delete")
	}
	if command.Permanent && len(m.state.ReferencedBy(command.Subject, command.Version)) > 0 {
		return Result{}, domainError(ErrorConflict, "version", "active schemas reference this version")
	}
	result := Result{Version: command.Version}
	envelope, err := newEnvelope(m.state.Sequence()+1, command.OperationID, TransitionVersionDeleted, deleteVersionTransition{
		Subject: command.Subject, Version: command.Version, Permanent: command.Permanent,
	}, result)
	if err != nil {
		return Result{}, err
	}
	return m.commitApply(ctx, envelope, result)
}

// DeleteSubject soft- or permanently deletes all applicable versions.
func (m *Machine) DeleteSubject(ctx context.Context, command DeleteSubjectCommand) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if result, ok := m.state.ResultFor(command.OperationID); command.OperationID != "" && ok {
		return result, nil
	}
	if m.state.EffectiveMode(command.Subject) == ModeReadOnly {
		return Result{}, domainError(ErrorReadOnly, command.Subject, "deletion is disabled")
	}
	versions := m.state.Versions(command.Subject, command.Permanent)
	if len(versions) == 0 {
		return Result{}, domainError(ErrorNotFound, "subject", "%s", command.Subject)
	}
	if command.Permanent {
		for _, version := range versions {
			item, _, _ := m.state.Lookup(command.Subject, version, true)
			if !item.Deleted {
				return Result{}, domainError(ErrorConflict, "subject", "soft delete is required before permanent delete")
			}
			if len(m.state.ReferencedBy(command.Subject, version)) > 0 {
				return Result{}, domainError(ErrorConflict, "subject", "active schemas reference version %d", version)
			}
		}
	}
	result := Result{Versions: versions}
	envelope, err := newEnvelope(m.state.Sequence()+1, command.OperationID, TransitionSubjectDeleted, deleteSubjectTransition{
		Subject: command.Subject, Versions: versions, Permanent: command.Permanent,
	}, result)
	if err != nil {
		return Result{}, err
	}
	return m.commitApply(ctx, envelope, result)
}

// SetCompatibility sets global or subject compatibility.
func (m *Machine) SetCompatibility(ctx context.Context, operationID OperationID, scope Scope, level CompatibilityLevel) error {
	if !level.Valid() {
		return domainError(ErrorInvalid, "compatibility", "unsupported level %q", level)
	}
	return m.simpleMutation(ctx, operationID, TransitionCompatibilitySet, compatibilityTransition{Scope: scope, Level: level})
}

// DeleteCompatibility removes subject compatibility so it inherits globally.
func (m *Machine) DeleteCompatibility(ctx context.Context, operationID OperationID, subject string) error {
	if subject == "" {
		return domainError(ErrorInvalid, "compatibility", "global compatibility cannot be deleted")
	}
	return m.simpleMutation(ctx, operationID, TransitionCompatibilityDel, compatibilityTransition{Scope: Scope{Subject: subject}})
}

// SetMode sets global or subject registry mode. Mode changes remain available in READONLY mode.
func (m *Machine) SetMode(ctx context.Context, operationID OperationID, scope Scope, mode Mode) error {
	if !mode.Valid() {
		return domainError(ErrorInvalid, "mode", "unsupported mode %q", mode)
	}
	return m.simpleMutation(ctx, operationID, TransitionModeSet, modeTransition{Scope: scope, Mode: mode})
}

// DeleteMode removes a subject mode so it inherits globally.
func (m *Machine) DeleteMode(ctx context.Context, operationID OperationID, subject string) error {
	if subject == "" {
		return domainError(ErrorInvalid, "mode", "global mode cannot be deleted")
	}
	return m.simpleMutation(ctx, operationID, TransitionModeDeleted, modeTransition{Scope: Scope{Subject: subject}})
}

func (m *Machine) simpleMutation(ctx context.Context, operationID OperationID, kind TransitionKind, payload any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.state.ResultFor(operationID); operationID != "" && ok {
		return nil
	}
	envelope, err := newEnvelope(m.state.Sequence()+1, operationID, kind, payload, Result{})
	if err != nil {
		return err
	}
	_, err = m.commitApply(ctx, envelope, Result{})
	return err
}

func (m *Machine) commitApply(ctx context.Context, envelope Envelope, result Result) (Result, error) {
	next, err := m.state.Apply(envelope)
	if err != nil {
		return Result{}, err
	}
	if err := m.commit.Commit(ctx, envelope); err != nil {
		return Result{}, &Error{Category: ErrorStorage, Resource: "transition", Detail: "durable commit failed", Cause: err}
	}
	m.state = next
	return cloneResult(result), nil
}

func (m *Machine) compatibilitySchemas(subject string) []Schema {
	versions := m.state.Versions(subject, false)
	if len(versions) == 0 {
		return nil
	}
	level := m.state.EffectiveCompatibility(subject)
	if !level.Transitive() {
		versions = versions[len(versions)-1:]
	}
	result := make([]Schema, 0, len(versions))
	for _, version := range versions {
		_, schema, ok := m.state.Lookup(subject, version, false)
		if ok {
			result = append(result, schema)
		}
	}
	return result
}

func nextVersion(versions []SubjectVersion) Version {
	var maximum Version
	for _, item := range versions {
		if item.Version > maximum {
			maximum = item.Version
		}
	}
	return maximum + 1
}

func validSchemaType(schemaType SchemaType) bool {
	return schemaType == SchemaTypeAvro || schemaType == SchemaTypeProtobuf || schemaType == SchemaTypeJSON
}
