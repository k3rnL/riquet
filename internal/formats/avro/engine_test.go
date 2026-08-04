package avro

import (
	"context"
	"errors"
	"testing"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
)

func TestParseValidationIdentityAndNormalization(t *testing.T) {
	t.Parallel()

	engine := Engine{}
	compact := `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}`
	spaced := `{ "fields": [ { "type": "long", "name": "id" } ], "name": "Event", "type": "record" }`
	first, err := engine.Parse(context.Background(), formats.ParseRequest{Definition: compact}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.Parse(context.Background(), formats.ParseRequest{Definition: spaced, Normalize: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.Identity != second.Identity {
		t.Fatalf("equivalent identities differ: %s != %s", first.Identity, second.Identity)
	}
	if second.Definition != second.Canonical || second.Definition == spaced {
		t.Fatalf("normalization did not return canonical definition: %q", second.Definition)
	}
	if _, err := engine.Parse(context.Background(), formats.ParseRequest{Definition: `{"type":"record"}`}, nil); err == nil {
		t.Fatal("invalid schema was accepted")
	}
}

func TestCanonicalDefinitionMatchesApacheAvroOrdering(t *testing.T) {
	t.Parallel()

	definition := `{ "fields": [{"type":"long","name":"id"}], "name":"Event", "type":"record" }`
	canonical, err := canonicalDefinition(definition)
	if err != nil {
		t.Fatal(err)
	}
	if canonical != `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}` {
		t.Fatalf("canonical definition = %s", canonical)
	}
	primitive, err := canonicalDefinition(`{"type":"string"}`)
	if err != nil || primitive != `"string"` {
		t.Fatalf("primitive canonical definition = %q, %v", primitive, err)
	}
	referenced, err := canonicalDefinition(`{"type":"record","name":"Envelope","namespace":"example","fields":[{"name":"common","type":"example.Common"}]}`)
	if err != nil || referenced != `{"type":"record","name":"Envelope","namespace":"example","fields":[{"name":"common","type":"Common"}]}` {
		t.Fatalf("referenced canonical definition = %q, %v", referenced, err)
	}
}

func TestParseResolvesNamedReference(t *testing.T) {
	t.Parallel()

	resolver := formats.ResolveFunc(func(_ context.Context, reference domain.Reference) (domain.Schema, error) {
		if reference.Subject != "common" || reference.Version != 1 {
			return domain.Schema{}, errors.New("not found")
		}
		return domain.Schema{
			Type:       domain.SchemaTypeAvro,
			Definition: `{"type":"record","name":"Common","namespace":"example","fields":[{"name":"value","type":"string"}]}`,
		}, nil
	})
	definition := `{"type":"record","name":"Event","namespace":"example","fields":[{"name":"common","type":"example.Common"}]}`
	parsed, err := (Engine{}).Parse(context.Background(), formats.ParseRequest{
		Definition: definition,
		References: []domain.Reference{{Name: "example.Common", Subject: "common", Version: 1}},
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Identity == "" {
		t.Fatal("reference parse returned empty identity")
	}
}

func TestParseBoundsReferenceGraph(t *testing.T) {
	t.Parallel()

	resolver := formats.ResolveFunc(func(_ context.Context, reference domain.Reference) (domain.Schema, error) {
		return domain.Schema{
			Type: domain.SchemaTypeAvro, Definition: `{"type":"record","name":"Node","fields":[]}`,
			References: []domain.Reference{{Name: "Node", Subject: reference.Subject, Version: reference.Version}},
		}, nil
	})
	_, err := (Engine{}).Parse(context.Background(), formats.ParseRequest{
		Definition: `"Node"`, References: []domain.Reference{{Name: "Node", Subject: "node", Version: 1}},
		Limits: formats.Limits{MaxSchemaBytes: 1024, MaxReferences: 10, MaxReferenceDepth: 10},
	}, resolver)
	if formatKind(err) != "reference_cycle" {
		t.Fatalf("reference cycle error = %v", err)
	}

	_, err = (Engine{}).Parse(context.Background(), formats.ParseRequest{
		Definition: `"string"`, Limits: formats.Limits{MaxSchemaBytes: 2, MaxReferences: 1, MaxReferenceDepth: 1},
	}, nil)
	if formatKind(err) != "schema_too_large" {
		t.Fatalf("size limit error = %v", err)
	}
}

func formatKind(err error) string {
	var formatErr *formats.Error
	if errors.As(err, &formatErr) {
		return formatErr.Kind
	}
	return ""
}

func TestCompatiblePolicies(t *testing.T) {
	t.Parallel()

	engine := Engine{}
	compatible, _, err := engine.Compatible(context.Background(), domain.CompatibilityNone, formats.Parsed{}, nil)
	if err != nil || !compatible {
		t.Fatalf("NONE compatibility = %v, %v", compatible, err)
	}
	oldSchema, err := engine.Parse(context.Background(), formats.ParseRequest{
		Definition: `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}`,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	withRequiredField, err := engine.Parse(context.Background(), formats.ParseRequest{
		Definition: `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}`,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	compatible, messages, err := engine.Compatible(context.Background(), domain.CompatibilityBackward, withRequiredField, []formats.Parsed{oldSchema})
	if err != nil || compatible || len(messages) == 0 {
		t.Fatalf("BACKWARD compatibility = %v, %v, %v", compatible, messages, err)
	}
	compatible, messages, err = engine.Compatible(context.Background(), domain.CompatibilityForward, withRequiredField, []formats.Parsed{oldSchema})
	if err != nil || !compatible || len(messages) != 0 {
		t.Fatalf("FORWARD compatibility = %v, %v, %v", compatible, messages, err)
	}
}
