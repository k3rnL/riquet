//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sr"
)

type goClientValue struct {
	Value string `json:"value"`
}

func TestMaintainedGoSchemaRegistryClientAndWireEnvelope(t *testing.T) {
	baseURL := startClientRegistry(t, "go-client.db")
	client, err := sr.NewClient(sr.URLs(baseURL))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	registered, err := client.CreateSchema(ctx, "go-value", sr.Schema{
		Type:   sr.TypeAvro,
		Schema: `{"type":"record","name":"GoValue","fields":[{"name":"value","type":"string"}]}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	serde := sr.NewSerde()
	serde.Register(registered.ID, goClientValue{},
		sr.EncodeFn(func(value any) ([]byte, error) { return json.Marshal(value) }),
		sr.DecodeFn(func(raw []byte, value any) error { return json.Unmarshal(raw, value) }),
	)
	wire, err := serde.Encode(goClientValue{Value: "go-value"})
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := serde.DecodeID(wire)
	if err != nil || id != registered.ID || len(wire) < 6 || wire[0] != 0 {
		t.Fatalf("Go Confluent envelope id/error/bytes = %d/%v/%v", id, err, wire)
	}
	var decoded goClientValue
	if err := serde.Decode(wire, &decoded); err != nil || decoded.Value != "go-value" {
		t.Fatalf("Go SerDe round trip = %+v, %v", decoded, err)
	}
	if schema, err := client.SchemaByID(ctx, id); err != nil || schema.Type != sr.TypeAvro {
		t.Fatalf("Go client schema lookup = %+v, %v", schema, err)
	}
}

func TestConfluentPythonClientAndWireEnvelope(t *testing.T) {
	baseURL := startClientRegistry(t, "python-client.db")
	script, err := filepath.Abs("../clients/python/client_interop.py")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "uv", "run", "--no-project", "--with", "confluent-kafka[avro]==2.12.2", "python", script)
	command.Env = append(os.Environ(), "RIQUET_REGISTRY_URL="+baseURL)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Confluent Python client: %v\n%s", err, output)
	} else {
		t.Logf("%s", output)
	}
}

func TestConfluentDotnetClientAndWireEnvelope(t *testing.T) {
	if testing.Short() {
		t.Skip(".NET SDK container test is disabled in short mode")
	}
	baseURL := startClientRegistry(t, "dotnet-client.db")
	clientDir, err := filepath.Abs("../clients/dotnet")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", "run", "--rm", "--network", "host",
		"--env", "RIQUET_REGISTRY_URL="+baseURL,
		"--volume", clientDir+":/source:ro",
		"mcr.microsoft.com/dotnet/sdk:8.0", "bash", "-lc",
		"cp -R /source /tmp/client && cd /tmp/client && dotnet run --configuration Release")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Confluent .NET client: %v\n%s", err, output)
	} else {
		t.Logf("%s", output)
	}
}
