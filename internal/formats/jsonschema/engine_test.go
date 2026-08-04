package jsonschema

import (
	"context"
	"errors"
	"testing"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
)

func TestParseDraftsNormalizationAndReferences(t *testing.T) {
	engine := Engine{}
	definition := `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"id":{"type":"integer"}}}`
	parsed, err := engine.Parse(context.Background(), formats.ParseRequest{Definition: definition, Normalize: true}, nil)
	if err != nil || parsed.Identity == "" {
		t.Fatalf("parse = %+v, %v", parsed, err)
	}
	if _, err := engine.Parse(context.Background(), formats.ParseRequest{Definition: `{"type":7}`}, nil); err == nil {
		t.Fatal("invalid schema accepted")
	}
	resolver := formats.ResolveFunc(func(_ context.Context, reference domain.Reference) (domain.Schema, error) {
		if reference.Subject != "common" {
			return domain.Schema{}, errors.New("not found")
		}
		return domain.Schema{Type: domain.SchemaTypeJSON, Definition: `{"type":"string"}`}, nil
	})
	if _, err := engine.Parse(context.Background(), formats.ParseRequest{
		Definition: `{"$ref":"common.json"}`, References: []domain.Reference{{Name: "common.json", Subject: "common", Version: 1}},
	}, resolver); err != nil {
		t.Fatal(err)
	}
}

func TestCompatibilityObjectConstraints(t *testing.T) {
	engine := Engine{}
	parse := func(definition string) formats.Parsed {
		parsed, err := engine.Parse(context.Background(), formats.ParseRequest{Definition: definition}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	oldSchema := parse(`{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"],"additionalProperties":false}`)
	compatibleCandidate := parse(`{"type":"object","properties":{"id":{"type":"integer"},"note":{"type":"string"}},"required":["id"],"additionalProperties":false}`)
	incompatibleCandidate := parse(`{"type":"object","properties":{"id":{"type":"integer"},"note":{"type":"string"}},"required":["id","note"],"additionalProperties":false}`)
	compatible, _, err := engine.Compatible(context.Background(), domain.CompatibilityBackward, compatibleCandidate, []formats.Parsed{oldSchema})
	if err != nil || !compatible {
		t.Fatalf("compatible = %v, %v", compatible, err)
	}
	compatible, messages, err := engine.Compatible(context.Background(), domain.CompatibilityBackward, incompatibleCandidate, []formats.Parsed{oldSchema})
	if err != nil || compatible || len(messages) == 0 {
		t.Fatalf("incompatible = %v, %v, %v", compatible, messages, err)
	}
}
