package formats_test

import (
	"context"
	"errors"
	"testing"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
	avroformat "github.com/k3rnL/riquet/internal/formats/avro"
	jsonformat "github.com/k3rnL/riquet/internal/formats/jsonschema"
	protobufformat "github.com/k3rnL/riquet/internal/formats/protobuf"
)

func TestCrossFormatIdentityAndResourceBounds(t *testing.T) {
	tests := []struct {
		name       string
		engine     formats.Engine
		definition string
	}{
		{name: "avro", engine: avroformat.Engine{}, definition: `"string"`},
		{name: "protobuf", engine: protobufformat.Engine{}, definition: `syntax = "proto3"; message Value { string value = 1; }`},
		{name: "json", engine: jsonformat.Engine{}, definition: `{"type":"string"}`},
	}
	identities := make(map[string]bool)
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			parsed, err := testCase.engine.Parse(context.Background(), formats.ParseRequest{Definition: testCase.definition}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if identities[parsed.Identity] {
				t.Fatalf("cross-format identity collision: %s", parsed.Identity)
			}
			identities[parsed.Identity] = true
			if _, err := testCase.engine.Parse(context.Background(), formats.ParseRequest{
				Definition: testCase.definition, Limits: formats.Limits{MaxSchemaBytes: 1, MaxReferences: 1, MaxReferenceDepth: 1},
			}, nil); err == nil {
				t.Fatal("schema-size limit was not enforced")
			}
		})
	}
}

func TestReferenceCycleAndTypeMismatchAreBounded(t *testing.T) {
	tests := []struct {
		name       string
		engine     formats.Engine
		definition string
		refName    string
		typeName   domain.SchemaType
	}{
		{name: "protobuf", engine: protobufformat.Engine{}, definition: `syntax = "proto3"; import "cycle.proto"; message Root { Cycle value = 1; }`, refName: "cycle.proto", typeName: domain.SchemaTypeProtobuf},
		{name: "json", engine: jsonformat.Engine{}, definition: `{"$ref":"cycle.json"}`, refName: "cycle.json", typeName: domain.SchemaTypeJSON},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			reference := domain.Reference{Name: testCase.refName, Subject: "cycle", Version: 1}
			resolver := formats.ResolveFunc(func(context.Context, domain.Reference) (domain.Schema, error) {
				return domain.Schema{Type: testCase.typeName, Definition: testCase.definition, References: []domain.Reference{reference}}, nil
			})
			if _, err := testCase.engine.Parse(context.Background(), formats.ParseRequest{
				Definition: testCase.definition, References: []domain.Reference{reference},
			}, resolver); err == nil {
				t.Fatal("reference cycle was accepted")
			}

			wrongTypeResolver := formats.ResolveFunc(func(context.Context, domain.Reference) (domain.Schema, error) {
				return domain.Schema{Type: domain.SchemaTypeAvro, Definition: `"string"`}, nil
			})
			if _, err := testCase.engine.Parse(context.Background(), formats.ParseRequest{
				Definition: testCase.definition, References: []domain.Reference{reference},
			}, wrongTypeResolver); err == nil {
				t.Fatal("cross-format reference was accepted")
			}
		})
	}
}

func TestMissingReferencesFailWithoutResolver(t *testing.T) {
	_, err := (jsonformat.Engine{}).Parse(context.Background(), formats.ParseRequest{
		Definition: `{"$ref":"missing.json"}`, References: []domain.Reference{{Name: "missing.json", Subject: "missing", Version: 1}},
	}, formats.ResolveFunc(func(context.Context, domain.Reference) (domain.Schema, error) {
		return domain.Schema{}, errors.New("missing")
	}))
	if err == nil {
		t.Fatal("missing reference was accepted")
	}
}
