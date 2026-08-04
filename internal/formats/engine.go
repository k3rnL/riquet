// Package formats contains schema-format engines and their shared contracts.
package formats

import (
	"context"

	"github.com/k3rnL/riquet/internal/domain"
)

// Limits bounds untrusted schema and reference graph work.
type Limits struct {
	MaxSchemaBytes    int
	MaxReferences     int
	MaxReferenceDepth int
}

// DefaultLimits returns conservative server defaults.
func DefaultLimits() Limits {
	return Limits{MaxSchemaBytes: 1 << 20, MaxReferences: 100, MaxReferenceDepth: 20}
}

// ParseRequest is one schema definition plus Confluent references.
type ParseRequest struct {
	Definition string
	References []domain.Reference
	Normalize  bool
	Limits     Limits
}

// Parsed is an engine-neutral schema plus an engine-owned parsed value.
type Parsed struct {
	Type       domain.SchemaType
	Definition string
	Canonical  string
	Identity   string
	References []domain.Reference
	Native     any
}

// Resolver returns an exact active schema referenced by subject and version.
type Resolver interface {
	Resolve(context.Context, domain.Reference) (domain.Schema, error)
}

// ResolveFunc adapts a function to Resolver.
type ResolveFunc func(context.Context, domain.Reference) (domain.Schema, error)

// Resolve calls the adapted function.
func (f ResolveFunc) Resolve(ctx context.Context, reference domain.Reference) (domain.Schema, error) {
	return f(ctx, reference)
}

// Engine parses, identifies, normalizes, resolves, and compares one format.
type Engine interface {
	Type() domain.SchemaType
	Parse(context.Context, ParseRequest, Resolver) (Parsed, error)
	Compatible(context.Context, domain.CompatibilityLevel, Parsed, []Parsed) (bool, []string, error)
}

// Error identifies bounded format validation failures.
type Error struct {
	Kind   string
	Detail string
	Cause  error
}

func (e *Error) Error() string { return e.Kind + ": " + e.Detail }
func (e *Error) Unwrap() error { return e.Cause }
