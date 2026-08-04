//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/contract"
	"github.com/k3rnL/riquet/internal/formats"
	avroformat "github.com/k3rnL/riquet/internal/formats/avro"
)

func TestAvroParserIdentityMatchesOracleCases(t *testing.T) {
	port := availablePort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	provisioner := contract.ComposeProvisioner{
		Name: "confluent-avro-parser", File: "compose.oracle.yml", Project: fmt.Sprintf("riquet-avro-%d", port),
		Environment: []string{fmt.Sprintf("RIQUET_ORACLE_PORT=%d", port)}, BaseURL: fmt.Sprintf("http://127.0.0.1:%d", port),
		ArtifactsDir: filepath.Join(t.TempDir(), "artifacts"),
	}
	target, err := provisioner.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Minute)
		defer closeCancel()
		if err := target.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})

	compact := `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}`
	reordered := `{ "fields": [ { "type": "long", "name": "id" } ], "name": "Event", "type": "record" }`
	engine := avroformat.Engine{}
	left, err := engine.Parse(ctx, formats.ParseRequest{Definition: compact}, nil)
	if err != nil {
		t.Fatal(err)
	}
	right, err := engine.Parse(ctx, formats.ParseRequest{Definition: reordered}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if left.Identity != right.Identity {
		t.Fatal("selected parser treats equivalent Avro definitions as distinct")
	}
	firstID := registerOracleSchema(t, ctx, target, "parser-value", compact, false)
	secondID := registerOracleSchema(t, ctx, target, "parser-value", reordered, false)
	if firstID != secondID {
		t.Fatalf("oracle identities differ for parser-equivalent schemas: %d != %d", firstID, secondID)
	}
	normalizedID := registerOracleSchema(t, ctx, target, "normalized-value", reordered, true)
	if normalizedID == 0 {
		t.Fatal("oracle normalization did not return an ID")
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.BaseURL().String()+"/subjects/invalid-value/versions", bytes.NewBufferString(`{"schema":"{\\\"type\\\":\\\"record\\\"}"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := target.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusUnprocessableEntity {
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		t.Fatalf("invalid Avro status = %d: %s", response.StatusCode, body)
	}
	_ = response.Body.Close()
}

func registerOracleSchema(t testing.TB, ctx context.Context, target contract.Target, subject, definition string, normalize bool) int64 {
	t.Helper()
	body, err := json.Marshal(map[string]string{"schema": definition})
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("%s/subjects/%s/versions?normalize=%t", target.BaseURL(), subject, normalize)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	response, err := target.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		t.Fatalf("register schema status = %d: %s", response.StatusCode, body)
	}
	var result struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		_ = response.Body.Close()
		t.Fatal(err)
	}
	_ = response.Body.Close()
	return result.ID
}
