package protobuf

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
)

func TestParseValidateCanonicalizeAndResolve(t *testing.T) {
	engine := Engine{}
	definition := `syntax = "proto3"; package example; message Event { int64 id = 1; }`
	parsed, err := engine.Parse(context.Background(), formats.ParseRequest{Definition: definition}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Identity == "" || !strings.Contains(parsed.Definition, "message Event") {
		t.Fatalf("parsed schema = %+v", parsed)
	}
	if _, err := engine.Parse(context.Background(), formats.ParseRequest{Definition: `message {`}, nil); err == nil {
		t.Fatal("invalid schema accepted")
	}

	resolver := formats.ResolveFunc(func(_ context.Context, reference domain.Reference) (domain.Schema, error) {
		if reference.Subject != "common" {
			return domain.Schema{}, errors.New("not found")
		}
		return domain.Schema{Type: domain.SchemaTypeProtobuf, Definition: `syntax = "proto3"; package example; message Common { string value = 1; }`}, nil
	})
	withImport := `syntax = "proto3"; package example; import "common.proto"; message Envelope { Common common = 1; }`
	if _, err := engine.Parse(context.Background(), formats.ParseRequest{
		Definition: withImport, References: []domain.Reference{{Name: "common.proto", Subject: "common", Version: 1}},
	}, resolver); err != nil {
		t.Fatal(err)
	}
}

func TestCompatibilityUsesWireClasses(t *testing.T) {
	engine := Engine{}
	parse := func(definition string) formats.Parsed {
		parsed, err := engine.Parse(context.Background(), formats.ParseRequest{Definition: definition}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	oldSchema := parse(`syntax = "proto3"; message Event { int32 value = 1; }`)
	compatibleChange := parse(`syntax = "proto3"; message Event { int64 value = 1; string note = 2; }`)
	incompatibleChange := parse(`syntax = "proto3"; message Event { string value = 1; }`)
	compatible, _, err := engine.Compatible(context.Background(), domain.CompatibilityBackward, compatibleChange, []formats.Parsed{oldSchema})
	if err != nil || !compatible {
		t.Fatalf("compatible wire change = %v, %v", compatible, err)
	}
	compatible, messages, err := engine.Compatible(context.Background(), domain.CompatibilityBackward, incompatibleChange, []formats.Parsed{oldSchema})
	if err != nil || compatible || len(messages) == 0 {
		t.Fatalf("incompatible wire change = %v, %v, %v", compatible, messages, err)
	}
}
