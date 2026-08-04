package domain

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
)

// Snapshot is a portable immutable representation of materialized state.
type Snapshot struct {
	FormatVersion        int                           `json:"formatVersion"`
	Sequence             Sequence                      `json:"sequence"`
	NextSchemaID         SchemaID                      `json:"nextSchemaId"`
	Schemas              map[SchemaID]Schema           `json:"schemas"`
	IdentityToID         map[string]SchemaID           `json:"identityToId"`
	Subjects             map[string][]SubjectVersion   `json:"subjects"`
	GlobalCompatibility  CompatibilityLevel            `json:"globalCompatibility"`
	SubjectCompatibility map[string]CompatibilityLevel `json:"subjectCompatibility"`
	GlobalMode           Mode                          `json:"globalMode"`
	SubjectModes         map[string]Mode               `json:"subjectModes"`
	Operations           map[OperationID]Result        `json:"operations"`
}

// State is an immutable registry materialized view.
type State struct{ snapshot Snapshot }

// NewState creates an empty registry with Confluent defaults.
func NewState() State {
	return State{snapshot: Snapshot{
		FormatVersion: 1, NextSchemaID: 1,
		Schemas: make(map[SchemaID]Schema), IdentityToID: make(map[string]SchemaID),
		Subjects:             make(map[string][]SubjectVersion),
		GlobalCompatibility:  CompatibilityBackward,
		SubjectCompatibility: make(map[string]CompatibilityLevel),
		GlobalMode:           ModeReadWrite, SubjectModes: make(map[string]Mode),
		Operations: make(map[OperationID]Result),
	}}
}

// Restore validates and restores a portable snapshot.
func Restore(snapshot Snapshot) (State, error) {
	if snapshot.FormatVersion != 1 {
		return State{}, domainError(ErrorCorrupt, "snapshot", "unsupported format version %d", snapshot.FormatVersion)
	}
	state := State{snapshot: cloneSnapshot(snapshot)}
	if err := state.validate(); err != nil {
		return State{}, err
	}
	return state, nil
}

// Snapshot returns a deep copy suitable for serialization.
func (s State) Snapshot() Snapshot { return cloneSnapshot(s.snapshot) }

// Sequence returns the last applied transition sequence.
func (s State) Sequence() Sequence { return s.snapshot.Sequence }

// Apply validates and immutably applies the next transition.
func (s State) Apply(envelope Envelope) (State, error) {
	if err := envelope.Validate(); err != nil {
		return State{}, err
	}
	if envelope.Sequence != s.snapshot.Sequence+1 {
		return State{}, domainError(ErrorCorrupt, "transition", "sequence %d does not follow %d", envelope.Sequence, s.snapshot.Sequence)
	}
	next := cloneSnapshot(s.snapshot)
	if err := applyPayload(&next, envelope); err != nil {
		return State{}, err
	}
	next.Sequence = envelope.Sequence
	if envelope.OperationID != "" {
		next.Operations[envelope.OperationID] = cloneResult(envelope.Result)
	}
	state := State{snapshot: next}
	if err := state.validate(); err != nil {
		return State{}, err
	}
	return state, nil
}

// ResultFor returns a previously committed operation result.
func (s State) ResultFor(operationID OperationID) (Result, bool) {
	result, ok := s.snapshot.Operations[operationID]
	return cloneResult(result), ok
}

// SchemaByID returns a defensive copy.
func (s State) SchemaByID(id SchemaID) (Schema, bool) {
	schema, ok := s.snapshot.Schemas[id]
	return schema.Clone(), ok
}

// VersionsForID returns active subject versions using a global schema ID.
func (s State) VersionsForID(id SchemaID) []SubjectVersion {
	var result []SubjectVersion
	for _, versions := range s.snapshot.Subjects {
		for _, item := range versions {
			if item.SchemaID == id && !item.Deleted && !item.Permanent {
				result = append(result, item)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Sequence != result[j].Sequence {
			return result[i].Sequence > result[j].Sequence
		}
		if result[i].Subject == result[j].Subject {
			return result[i].Version > result[j].Version
		}
		return result[i].Subject > result[j].Subject
	})
	return result
}

// LatestForID returns the most recently committed active association for an ID.
func (s State) LatestForID(id SchemaID, subject string) (SubjectVersion, bool) {
	var latest SubjectVersion
	found := false
	for owner, versions := range s.snapshot.Subjects {
		if subject != "" && owner != subject {
			continue
		}
		for _, item := range versions {
			if item.SchemaID != id || item.Deleted || item.Permanent {
				continue
			}
			if !found || item.Sequence > latest.Sequence {
				latest = item
				found = true
			}
		}
	}
	return latest, found
}

// Subjects lists subject names, excluding fully soft-deleted subjects unless requested.
func (s State) Subjects(includeDeleted bool) []string {
	result := make([]string, 0, len(s.snapshot.Subjects))
	for subject, versions := range s.snapshot.Subjects {
		for _, item := range versions {
			if item.Permanent || (!includeDeleted && item.Deleted) {
				continue
			}
			result = append(result, subject)
			break
		}
	}
	sort.Strings(result)
	return result
}

// Versions lists versions for a subject in ascending order.
func (s State) Versions(subject string, includeDeleted bool) []Version {
	var result []Version
	for _, item := range s.snapshot.Subjects[subject] {
		if item.Permanent || (!includeDeleted && item.Deleted) {
			continue
		}
		result = append(result, item.Version)
	}
	slices.Sort(result)
	return result
}

// Lookup returns a subject version and its schema.
func (s State) Lookup(subject string, version Version, includeDeleted bool) (SubjectVersion, Schema, bool) {
	for _, item := range s.snapshot.Subjects[subject] {
		if item.Version != version || item.Permanent || (!includeDeleted && item.Deleted) {
			continue
		}
		schema, ok := s.snapshot.Schemas[item.SchemaID]
		return item, schema.Clone(), ok
	}
	return SubjectVersion{}, Schema{}, false
}

// FindSubjectSchema returns an active subject version by schema identity.
func (s State) FindSubjectSchema(subject, identity string) (SubjectVersion, Schema, bool) {
	for _, item := range s.snapshot.Subjects[subject] {
		if item.Deleted || item.Permanent {
			continue
		}
		schema, ok := s.snapshot.Schemas[item.SchemaID]
		if ok && schema.Identity == identity {
			return item, schema.Clone(), true
		}
	}
	return SubjectVersion{}, Schema{}, false
}

// EffectiveCompatibility returns subject configuration or the global default.
func (s State) EffectiveCompatibility(subject string) CompatibilityLevel {
	if level, ok := s.snapshot.SubjectCompatibility[subject]; ok {
		return level
	}
	return s.snapshot.GlobalCompatibility
}

// GlobalCompatibility returns the registry default compatibility.
func (s State) GlobalCompatibility() CompatibilityLevel { return s.snapshot.GlobalCompatibility }

// SubjectCompatibility returns an explicitly configured subject value.
func (s State) SubjectCompatibility(subject string) (CompatibilityLevel, bool) {
	level, ok := s.snapshot.SubjectCompatibility[subject]
	return level, ok
}

// EffectiveMode returns subject mode or the global default.
func (s State) EffectiveMode(subject string) Mode {
	if mode, ok := s.snapshot.SubjectModes[subject]; ok {
		return mode
	}
	return s.snapshot.GlobalMode
}

// GlobalMode returns the registry default mode.
func (s State) GlobalMode() Mode { return s.snapshot.GlobalMode }

// SubjectMode returns an explicitly configured subject mode.
func (s State) SubjectMode(subject string) (Mode, bool) {
	mode, ok := s.snapshot.SubjectModes[subject]
	return mode, ok
}

// ReferencedBy returns active subject versions that reference the target.
func (s State) ReferencedBy(subject string, version Version) []SubjectVersion {
	var result []SubjectVersion
	for owner, versions := range s.snapshot.Subjects {
		for _, item := range versions {
			if item.Deleted || item.Permanent {
				continue
			}
			schema := s.snapshot.Schemas[item.SchemaID]
			for _, reference := range schema.References {
				if reference.Subject == subject && reference.Version == version {
					copyItem := item
					copyItem.Subject = owner
					result = append(result, copyItem)
					break
				}
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Subject == result[j].Subject {
			return result[i].Version < result[j].Version
		}
		return result[i].Subject < result[j].Subject
	})
	return result
}

func applyPayload(snapshot *Snapshot, envelope Envelope) error {
	switch envelope.Kind {
	case TransitionSchemaRegistered:
		var payload registrationTransition
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return corruptPayload(err)
		}
		if existing, ok := snapshot.Schemas[payload.Schema.ID]; ok && existing.Identity != payload.Schema.Identity {
			return domainError(ErrorCorrupt, "schema", "ID %d identity conflict", payload.Schema.ID)
		}
		snapshot.Schemas[payload.Schema.ID] = payload.Schema.Clone()
		snapshot.IdentityToID[payload.Schema.Identity] = payload.Schema.ID
		snapshot.Subjects[payload.Version.Subject] = append(snapshot.Subjects[payload.Version.Subject], payload.Version)
		if payload.Schema.ID >= snapshot.NextSchemaID {
			snapshot.NextSchemaID = payload.Schema.ID + 1
		}
	case TransitionVersionDeleted:
		var payload deleteVersionTransition
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return corruptPayload(err)
		}
		if !markDeleted(snapshot, payload.Subject, payload.Version, payload.Permanent) {
			return domainError(ErrorCorrupt, "version", "missing %s/%d", payload.Subject, payload.Version)
		}
	case TransitionSubjectDeleted:
		var payload deleteSubjectTransition
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return corruptPayload(err)
		}
		for _, version := range payload.Versions {
			if !markDeleted(snapshot, payload.Subject, version, payload.Permanent) {
				return domainError(ErrorCorrupt, "version", "missing %s/%d", payload.Subject, version)
			}
		}
	case TransitionCompatibilitySet, TransitionCompatibilityDel:
		var payload compatibilityTransition
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return corruptPayload(err)
		}
		switch {
		case envelope.Kind == TransitionCompatibilityDel:
			delete(snapshot.SubjectCompatibility, payload.Scope.Subject)
		case payload.Scope.Subject == "":
			snapshot.GlobalCompatibility = payload.Level
		default:
			snapshot.SubjectCompatibility[payload.Scope.Subject] = payload.Level
		}
	case TransitionModeSet, TransitionModeDeleted:
		var payload modeTransition
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return corruptPayload(err)
		}
		switch {
		case envelope.Kind == TransitionModeDeleted:
			delete(snapshot.SubjectModes, payload.Scope.Subject)
		case payload.Scope.Subject == "":
			snapshot.GlobalMode = payload.Mode
		default:
			snapshot.SubjectModes[payload.Scope.Subject] = payload.Mode
		}
	default:
		return domainError(ErrorCorrupt, "transition", "unknown kind %q", envelope.Kind)
	}
	return nil
}

func markDeleted(snapshot *Snapshot, subject string, version Version, permanent bool) bool {
	versions := snapshot.Subjects[subject]
	for index := range versions {
		if versions[index].Version == version && !versions[index].Permanent {
			versions[index].Deleted = true
			versions[index].Permanent = permanent
			snapshot.Subjects[subject] = versions
			return true
		}
	}
	return false
}

func (s State) validate() error {
	if s.snapshot.NextSchemaID < 1 || !s.snapshot.GlobalCompatibility.Valid() || !s.snapshot.GlobalMode.Valid() {
		return domainError(ErrorCorrupt, "snapshot", "invalid defaults")
	}
	for identity, id := range s.snapshot.IdentityToID {
		if schema, ok := s.snapshot.Schemas[id]; !ok || schema.Identity != identity {
			return domainError(ErrorCorrupt, "snapshot", "identity index mismatch")
		}
	}
	for subject, versions := range s.snapshot.Subjects {
		seen := make(map[Version]bool, len(versions))
		for _, item := range versions {
			if item.Subject != subject || item.Version < 1 || seen[item.Version] {
				return domainError(ErrorCorrupt, "snapshot", "invalid subject version index")
			}
			seen[item.Version] = true
			if _, ok := s.snapshot.Schemas[item.SchemaID]; !ok {
				return domainError(ErrorCorrupt, "snapshot", "version references missing schema")
			}
		}
	}
	return nil
}

func cloneSnapshot(source Snapshot) Snapshot {
	result := source
	result.Schemas = make(map[SchemaID]Schema, len(source.Schemas))
	for key, value := range source.Schemas {
		result.Schemas[key] = value.Clone()
	}
	result.IdentityToID = maps.Clone(source.IdentityToID)
	result.Subjects = make(map[string][]SubjectVersion, len(source.Subjects))
	for key, value := range source.Subjects {
		result.Subjects[key] = slices.Clone(value)
	}
	result.SubjectCompatibility = maps.Clone(source.SubjectCompatibility)
	result.SubjectModes = maps.Clone(source.SubjectModes)
	result.Operations = make(map[OperationID]Result, len(source.Operations))
	for key, value := range source.Operations {
		result.Operations[key] = cloneResult(value)
	}
	return result
}

func cloneResult(result Result) Result {
	result.Versions = slices.Clone(result.Versions)
	return result
}

func corruptPayload(err error) error {
	return &Error{Category: ErrorCorrupt, Resource: "transition payload", Detail: fmt.Sprintf("invalid JSON: %v", err), Cause: err}
}
