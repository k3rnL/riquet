// Package avro implements Avro parsing and Confluent identity behavior.
package avro

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	hamba "github.com/hamba/avro/v2"
	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
)

// Engine parses Avro with hamba/avro and owns Confluent-specific behavior.
type Engine struct{}

// Type returns the Confluent Avro schema type.
func (Engine) Type() domain.SchemaType { return domain.SchemaTypeAvro }

// Parse validates the reference graph and computes stable canonical identity.
func (Engine) Parse(ctx context.Context, request formats.ParseRequest, resolver formats.Resolver) (formats.Parsed, error) {
	limits := normalizedLimits(request.Limits)
	if len(request.Definition) == 0 {
		return formats.Parsed{}, &formats.Error{Kind: "invalid_avro", Detail: "schema is empty"}
	}
	if len(request.Definition) > limits.MaxSchemaBytes {
		return formats.Parsed{}, &formats.Error{Kind: "schema_too_large", Detail: fmt.Sprintf("schema exceeds %d bytes", limits.MaxSchemaBytes)}
	}
	if len(request.References) > limits.MaxReferences {
		return formats.Parsed{}, &formats.Error{Kind: "too_many_references", Detail: fmt.Sprintf("reference count exceeds %d", limits.MaxReferences)}
	}
	cache := &hamba.SchemaCache{}
	state := resolutionState{limits: limits, resolver: resolver, cache: cache, active: make(map[string]bool)}
	for _, reference := range request.References {
		if err := state.load(ctx, reference, 1); err != nil {
			return formats.Parsed{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return formats.Parsed{}, err
	}
	parsed, err := hamba.ParseWithCache(request.Definition, "", cache)
	if err != nil {
		return formats.Parsed{}, &formats.Error{Kind: "invalid_avro", Detail: err.Error(), Cause: err}
	}
	parserCanonical := parsed.String()
	definition, err := canonicalDefinition(request.Definition)
	if err != nil {
		return formats.Parsed{}, &formats.Error{Kind: "invalid_avro", Detail: err.Error(), Cause: err}
	}
	if request.Normalize {
		definition = parserCanonical
	}
	identity, err := schemaIdentity(parserCanonical, request.References)
	if err != nil {
		return formats.Parsed{}, err
	}
	return formats.Parsed{
		Type: domain.SchemaTypeAvro, Definition: definition, Canonical: parserCanonical,
		Identity: identity, References: slices.Clone(request.References), Native: parsed,
	}, nil
}

// Compatible applies Confluent's reader/writer direction for the configured policy.
func (Engine) Compatible(ctx context.Context, level domain.CompatibilityLevel, candidate formats.Parsed, previous []formats.Parsed) (bool, []string, error) {
	if !level.Valid() {
		return false, nil, &formats.Error{Kind: "invalid_compatibility", Detail: fmt.Sprintf("unsupported level %q", level)}
	}
	if level == domain.CompatibilityNone {
		return true, nil, nil
	}
	candidateSchema, ok := candidate.Native.(hamba.Schema)
	if !ok || candidateSchema == nil {
		return false, nil, &formats.Error{Kind: "invalid_compatibility", Detail: "candidate is not parsed Avro"}
	}
	var messages []string
	for index, item := range previous {
		if err := ctx.Err(); err != nil {
			return false, nil, err
		}
		previousSchema, previousOK := item.Native.(hamba.Schema)
		if !previousOK || previousSchema == nil {
			return false, nil, &formats.Error{Kind: "invalid_compatibility", Detail: fmt.Sprintf("previous schema %d is not parsed Avro", index)}
		}
		switch level {
		case domain.CompatibilityNone:
			continue
		case domain.CompatibilityBackward, domain.CompatibilityBackwardTransitive:
			messages = appendCompatibility(messages, index, candidateSchema, previousSchema)
		case domain.CompatibilityForward, domain.CompatibilityForwardTransitive:
			messages = appendCompatibility(messages, index, previousSchema, candidateSchema)
		case domain.CompatibilityFull, domain.CompatibilityFullTransitive:
			messages = appendCompatibility(messages, index, candidateSchema, previousSchema)
			messages = appendCompatibility(messages, index, previousSchema, candidateSchema)
		}
	}
	return len(messages) == 0, messages, nil
}

func appendCompatibility(messages []string, previousIndex int, reader, writer hamba.Schema) []string {
	if err := hamba.NewSchemaCompatibility().Compatible(reader, writer); err != nil {
		return append(messages, fmt.Sprintf("previous schema %d: %v", previousIndex+1, err))
	}
	return messages
}

type resolutionState struct {
	limits   formats.Limits
	resolver formats.Resolver
	cache    *hamba.SchemaCache
	count    int
	active   map[string]bool
}

func (s *resolutionState) load(ctx context.Context, reference domain.Reference, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.resolver == nil {
		return &formats.Error{Kind: "missing_reference", Detail: fmt.Sprintf("no resolver for %s/%d", reference.Subject, reference.Version)}
	}
	if depth > s.limits.MaxReferenceDepth {
		return &formats.Error{Kind: "reference_depth", Detail: fmt.Sprintf("reference depth exceeds %d", s.limits.MaxReferenceDepth)}
	}
	s.count++
	if s.count > s.limits.MaxReferences {
		return &formats.Error{Kind: "too_many_references", Detail: fmt.Sprintf("resolved reference count exceeds %d", s.limits.MaxReferences)}
	}
	key := fmt.Sprintf("%s/%d", reference.Subject, reference.Version)
	if s.active[key] {
		return &formats.Error{Kind: "reference_cycle", Detail: "cycle at " + key}
	}
	s.active[key] = true
	defer delete(s.active, key)
	schema, err := s.resolver.Resolve(ctx, reference)
	if err != nil {
		return &formats.Error{Kind: "missing_reference", Detail: key, Cause: err}
	}
	if schema.Type != domain.SchemaTypeAvro {
		return &formats.Error{Kind: "reference_type", Detail: fmt.Sprintf("%s is %s, expected AVRO", key, schema.Type)}
	}
	if len(schema.Definition) > s.limits.MaxSchemaBytes {
		return &formats.Error{Kind: "schema_too_large", Detail: fmt.Sprintf("referenced schema %s exceeds %d bytes", key, s.limits.MaxSchemaBytes)}
	}
	for _, nested := range schema.References {
		if err := s.load(ctx, nested, depth+1); err != nil {
			return err
		}
	}
	parsed, err := hamba.ParseWithCache(schema.Definition, "", s.cache)
	if err != nil {
		return &formats.Error{Kind: "invalid_reference", Detail: key + ": " + err.Error(), Cause: err}
	}
	s.cache.Add(reference.Name, parsed)
	if named, ok := parsed.(interface {
		Name() string
		FullName() string
	}); ok {
		s.cache.Add(named.Name(), parsed)
		s.cache.Add(named.FullName(), parsed)
	}
	return nil
}

func normalizedLimits(limits formats.Limits) formats.Limits {
	defaults := formats.DefaultLimits()
	if limits.MaxSchemaBytes <= 0 {
		limits.MaxSchemaBytes = defaults.MaxSchemaBytes
	}
	if limits.MaxReferences <= 0 {
		limits.MaxReferences = defaults.MaxReferences
	}
	if limits.MaxReferenceDepth <= 0 {
		limits.MaxReferenceDepth = defaults.MaxReferenceDepth
	}
	return limits
}

func schemaIdentity(canonical string, references []domain.Reference) (string, error) {
	referenceJSON, err := json.Marshal(references)
	if err != nil {
		return "", &formats.Error{Kind: "identity", Detail: err.Error(), Cause: err}
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain.SchemaTypeAvro))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(canonical))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(referenceJSON)
	return hex.EncodeToString(hash.Sum(nil)), nil
}
