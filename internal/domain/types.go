// Package domain contains deterministic registry state and transitions.
package domain

import (
	"crypto/md5" // #nosec G501 -- Confluent defines schema GUIDs as MD5 fingerprints.
	"encoding/binary"
	"fmt"
	"slices"
)

// SchemaID is a registry-wide Confluent schema identifier.
type SchemaID int64

// SchemaGUID is the registry-independent Confluent schema fingerprint.
type SchemaGUID string

// Version is a monotonically increasing version within one subject.
type Version int64

// Sequence orders durable registry transitions.
type Sequence uint64

// OperationID identifies a mutation across ambiguous client retries.
type OperationID string

// SchemaType identifies one Confluent schema family.
type SchemaType string

const (
	// SchemaTypeAvro identifies Apache Avro schemas.
	SchemaTypeAvro SchemaType = "AVRO"
	// SchemaTypeProtobuf identifies Protocol Buffer schemas.
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
	// SchemaTypeJSON identifies JSON Schema definitions.
	SchemaTypeJSON SchemaType = "JSON"
)

// Reference points to an exact subject version.
type Reference struct {
	Name    string  `json:"name"`
	Subject string  `json:"subject"`
	Version Version `json:"version"`
}

// Schema is the persisted, format-validated schema representation.
type Schema struct {
	ID         SchemaID    `json:"id"`
	GUID       SchemaGUID  `json:"guid,omitempty"`
	Identity   string      `json:"identity"`
	Type       SchemaType  `json:"schemaType"`
	Definition string      `json:"schema"`
	References []Reference `json:"references,omitempty"`
}

// Clone returns an independent schema value.
func (s Schema) Clone() Schema {
	s.References = slices.Clone(s.References)
	return s
}

// SubjectVersion associates a global schema with a subject version.
type SubjectVersion struct {
	Subject   string   `json:"subject"`
	Version   Version  `json:"version"`
	SchemaID  SchemaID `json:"id"`
	Timestamp int64    `json:"ts,omitempty"`
	Sequence  Sequence `json:"sequence,omitempty"`
	Deleted   bool     `json:"deleted"`
	Permanent bool     `json:"permanent"`
}

// CompatibilityLevel controls schema evolution checks.
type CompatibilityLevel string

const (
	// CompatibilityNone disables compatibility checks.
	CompatibilityNone CompatibilityLevel = "NONE"
	// CompatibilityBackward checks new readers against the latest writer.
	CompatibilityBackward CompatibilityLevel = "BACKWARD"
	// CompatibilityBackwardTransitive checks new readers against all writers.
	CompatibilityBackwardTransitive CompatibilityLevel = "BACKWARD_TRANSITIVE"
	// CompatibilityForward checks the latest reader against new writers.
	CompatibilityForward CompatibilityLevel = "FORWARD"
	// CompatibilityForwardTransitive checks all readers against new writers.
	CompatibilityForwardTransitive CompatibilityLevel = "FORWARD_TRANSITIVE"
	// CompatibilityFull requires backward and forward compatibility.
	CompatibilityFull CompatibilityLevel = "FULL"
	// CompatibilityFullTransitive requires full compatibility with all versions.
	CompatibilityFullTransitive CompatibilityLevel = "FULL_TRANSITIVE"
)

// Valid reports whether the compatibility level is supported.
func (l CompatibilityLevel) Valid() bool {
	switch l {
	case CompatibilityNone, CompatibilityBackward, CompatibilityBackwardTransitive,
		CompatibilityForward, CompatibilityForwardTransitive, CompatibilityFull, CompatibilityFullTransitive:
		return true
	default:
		return false
	}
}

// Transitive reports whether the policy applies to every prior active version.
func (l CompatibilityLevel) Transitive() bool {
	return l == CompatibilityBackwardTransitive || l == CompatibilityForwardTransitive || l == CompatibilityFullTransitive
}

// Mode controls registry mutation behavior.
type Mode string

const (
	// ModeReadWrite permits normal registry mutations.
	ModeReadWrite Mode = "READWRITE"
	// ModeReadOnly rejects schema and deletion mutations.
	ModeReadOnly Mode = "READONLY"
	// ModeImport permits migration with explicit identifiers.
	ModeImport Mode = "IMPORT"
)

// Valid reports whether the mode is supported in the v1 contract.
func (m Mode) Valid() bool {
	return m == ModeReadWrite || m == ModeReadOnly || m == ModeImport
}

// Result is the stable outcome retained for operation retry idempotency.
type Result struct {
	SchemaID SchemaID   `json:"id,omitempty"`
	GUID     SchemaGUID `json:"guid,omitempty"`
	Version  Version    `json:"version,omitempty"`
	Versions []Version  `json:"versions,omitempty"`
}

// FingerprintGUID reproduces Confluent's schema GUID algorithm for a canonical
// schema without metadata or rules: schema bytes followed by each reference's
// name, subject, and signed 32-bit big-endian version.
func FingerprintGUID(definition string, references []Reference) SchemaGUID {
	hash := md5.New() // #nosec G401 -- wire compatibility requires MD5 here.
	_, _ = hash.Write([]byte(definition))
	var version [4]byte
	for _, reference := range references {
		_, _ = hash.Write([]byte(reference.Name))
		_, _ = hash.Write([]byte(reference.Subject))
		binary.BigEndian.PutUint32(version[:], uint32(reference.Version))
		_, _ = hash.Write(version[:])
	}
	sum := hash.Sum(nil)
	return SchemaGUID(fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16]))
}

// RegisterCommand describes a fully parsed and identified schema registration.
type RegisterCommand struct {
	OperationID      OperationID
	Subject          string
	Identity         string
	Type             SchemaType
	Definition       string
	References       []Reference
	RequestedID      SchemaID
	RequestedVersion Version
	Timestamp        int64
}

// DeleteVersionCommand removes one subject version.
type DeleteVersionCommand struct {
	OperationID OperationID
	Subject     string
	Version     Version
	Permanent   bool
}

// DeleteSubjectCommand removes every active or soft-deleted version of a subject.
type DeleteSubjectCommand struct {
	OperationID OperationID
	Subject     string
	Permanent   bool
}

// Scope identifies global configuration when Subject is empty.
type Scope struct {
	Subject string `json:"subject,omitempty"`
}
