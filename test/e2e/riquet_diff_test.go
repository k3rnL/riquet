//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/contract"
)

type avroEvolutionCase struct {
	Name             string            `json:"name"`
	Level            string            `json:"level"`
	Previous         []string          `json:"previous"`
	Candidate        string            `json:"candidate"`
	Compatible       bool              `json:"compatible"`
	References       []json.RawMessage `json:"references"`
	ReferenceSchemas map[string]string `json:"referenceSchemas"`
}

func TestRiquetDiscoveryMatchesPinnedOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	artifacts := os.Getenv("RIQUET_TEST_ARTIFACTS")
	if artifacts == "" {
		artifacts = filepath.Join(t.TempDir(), "artifacts")
	}
	binary := filepath.Join(t.TempDir(), "riquet")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../../cmd/riquet")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Riquet: %v: %s", err, output)
	}

	files, err := filepath.Glob("scenarios/discovery/*.json")
	if err != nil {
		t.Fatal(err)
	}
	options := contract.CompareOptions{
		RelevantHeaders: []string{"Content-Type"},
		SymbolicFields:  map[string]string{"id": "schema-id", "guid": "schema-guid"},
		OpaqueFields:    map[string]bool{"message": true, "messages": true, "ts": true},
	}
	for _, file := range files {
		file := file
		t.Run(strings.TrimSuffix(filepath.Base(file), filepath.Ext(file)), func(t *testing.T) {
			scenario, loadErr := contract.LoadScenario(file)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			scenarioArtifacts := filepath.Join(artifacts, scenario.Name)
			oracle, riquet := startDifferentialTargets(t, ctx, binary, scenarioArtifacts, "riquet-diff")
			reference, runErr := scenario.RunPrepared(ctx, oracle)
			if runErr != nil {
				t.Fatalf("oracle %s: %v", scenario.Name, runErr)
			}
			candidate, runErr := scenario.RunPrepared(ctx, riquet)
			if runErr != nil {
				t.Fatalf("Riquet %s: %v", scenario.Name, runErr)
			}
			if difference := contract.CompareTraces(reference, candidate, options); difference != nil {
				report := contract.NewReport(scenario.Name, oracle.Name(), riquet.Name(), "8.3.0", "compatibility/manifest.json", difference, reference, candidate)
				_ = contract.WriteReport(filepath.Join(scenarioArtifacts, scenario.Name+".diff.json"), report)
				t.Fatalf("%s exchange %d differs at %s: %s; reference=%s candidate=%s",
					scenario.Name, difference.Exchange, difference.Path, difference.Message,
					reference.Exchanges[difference.Exchange].ResponseBody, candidate.Exchanges[difference.Exchange].ResponseBody)
			}
		})
	}
}

func startDifferentialTargets(t *testing.T, ctx context.Context, binary, artifacts, projectPrefix string) (contract.Target, contract.Target) {
	t.Helper()
	oraclePort := availablePort(t)
	oracle, err := (contract.ComposeProvisioner{
		Name: "confluent-8.3.0", File: "compose.oracle.yml", Project: fmt.Sprintf("%s-%d", projectPrefix, oraclePort),
		Environment: []string{fmt.Sprintf("RIQUET_ORACLE_PORT=%d", oraclePort)},
		BaseURL:     fmt.Sprintf("http://127.0.0.1:%d", oraclePort), ArtifactsDir: artifacts,
	}).Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTarget(t, oracle) })

	riquetPort := availablePort(t)
	riquet, err := (contract.ProcessProvisioner{
		Name: "riquet", Executable: binary,
		Args:    []string{"-listen", fmt.Sprintf("127.0.0.1:%d", riquetPort), "-data", filepath.Join(t.TempDir(), "riquet.db")},
		BaseURL: fmt.Sprintf("http://127.0.0.1:%d", riquetPort), ArtifactsDir: artifacts,
	}).Start(ctx)
	if err != nil {
		closeTarget(t, oracle)
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTarget(t, riquet) })
	return oracle, riquet
}

func TestAvroEvolutionMatchesPinnedOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	artifacts := filepath.Join(t.TempDir(), "artifacts")
	binary := filepath.Join(t.TempDir(), "riquet")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../../cmd/riquet")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Riquet: %v: %s", err, output)
	}

	oraclePort := availablePort(t)
	oracle, err := (contract.ComposeProvisioner{
		Name: "confluent-8.3.0", File: "compose.oracle.yml", Project: fmt.Sprintf("riquet-evolution-%d", oraclePort),
		Environment: []string{fmt.Sprintf("RIQUET_ORACLE_PORT=%d", oraclePort)},
		BaseURL:     fmt.Sprintf("http://127.0.0.1:%d", oraclePort), ArtifactsDir: artifacts,
	}).Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTarget(t, oracle) })

	riquetPort := availablePort(t)
	riquet, err := (contract.ProcessProvisioner{
		Name: "riquet", Executable: binary,
		Args:    []string{"-listen", fmt.Sprintf("127.0.0.1:%d", riquetPort), "-data", filepath.Join(t.TempDir(), "riquet.db")},
		BaseURL: fmt.Sprintf("http://127.0.0.1:%d", riquetPort), ArtifactsDir: artifacts,
	}).Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTarget(t, riquet) })

	raw, err := os.ReadFile("../../internal/formats/avro/testdata/evolution.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []avroEvolutionCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for index, testCase := range cases {
		subject := fmt.Sprintf("evolution-%02d", index)
		oracleDecision := runEvolutionCase(t, ctx, oracle, subject, "AVRO", testCase)
		riquetDecision := runEvolutionCase(t, ctx, riquet, subject, "AVRO", testCase)
		if oracleDecision != testCase.Compatible {
			t.Fatalf("oracle %s decision = %v, corpus expects %v", testCase.Name, oracleDecision, testCase.Compatible)
		}
		if riquetDecision != oracleDecision {
			t.Fatalf("Riquet %s decision = %v, oracle = %v", testCase.Name, riquetDecision, oracleDecision)
		}
	}
}

func TestProtobufEvolutionMatchesPinnedOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "riquet")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../../cmd/riquet")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Riquet: %v: %s", err, output)
	}
	oracle, riquet := startDifferentialTargets(t, ctx, binary, filepath.Join(t.TempDir(), "artifacts"), "riquet-proto-evolution")
	raw, err := os.ReadFile("../../internal/formats/protobuf/testdata/evolution.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []avroEvolutionCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for index, testCase := range cases {
		subject := fmt.Sprintf("proto-evolution-%02d", index)
		oracleDecision := runEvolutionCase(t, ctx, oracle, subject, "PROTOBUF", testCase)
		riquetDecision := runEvolutionCase(t, ctx, riquet, subject, "PROTOBUF", testCase)
		if oracleDecision != testCase.Compatible {
			t.Fatalf("oracle %s decision = %v, corpus expects %v", testCase.Name, oracleDecision, testCase.Compatible)
		}
		if riquetDecision != oracleDecision {
			t.Fatalf("Riquet %s decision = %v, oracle = %v", testCase.Name, riquetDecision, oracleDecision)
		}
	}
}

func TestJSONSchemaEvolutionMatchesPinnedOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "riquet")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../../cmd/riquet")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Riquet: %v: %s", err, output)
	}
	oracle, riquet := startDifferentialTargets(t, ctx, binary, filepath.Join(t.TempDir(), "artifacts"), "riquet-json-evolution")
	raw, err := os.ReadFile("../../internal/formats/jsonschema/testdata/evolution.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []avroEvolutionCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for index, testCase := range cases {
		subject := fmt.Sprintf("json-evolution-%02d", index)
		oracleDecision := runEvolutionCase(t, ctx, oracle, subject, "JSON", testCase)
		riquetDecision := runEvolutionCase(t, ctx, riquet, subject, "JSON", testCase)
		if oracleDecision != testCase.Compatible {
			t.Fatalf("oracle %s decision = %v, corpus expects %v", testCase.Name, oracleDecision, testCase.Compatible)
		}
		if riquetDecision != oracleDecision {
			t.Fatalf("Riquet %s decision = %v, oracle = %v", testCase.Name, riquetDecision, oracleDecision)
		}
	}
}

func runEvolutionCase(t testing.TB, ctx context.Context, target contract.Target, subject, schemaType string, testCase avroEvolutionCase) bool {
	t.Helper()
	putTargetJSON(t, ctx, target, "/config/"+subject, map[string]any{"compatibility": "NONE"}, http.StatusOK)
	references := testCase.References
	if len(references) > 0 {
		var decoded []map[string]any
		encoded, _ := json.Marshal(references)
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		for referenceSubject, definition := range testCase.ReferenceSchemas {
			uniqueSubject := subject + "-" + referenceSubject
			postTargetJSON(t, ctx, target, "/subjects/"+uniqueSubject+"/versions", map[string]any{"schemaType": schemaType, "schema": definition}, http.StatusOK)
			for _, reference := range decoded {
				if reference["subject"] == referenceSubject {
					reference["subject"] = uniqueSubject
				}
			}
		}
		references = make([]json.RawMessage, len(decoded))
		for index, reference := range decoded {
			references[index], _ = json.Marshal(reference)
		}
	}
	for _, definition := range testCase.Previous {
		postTargetJSON(t, ctx, target, "/subjects/"+subject+"/versions", map[string]any{
			"schemaType": schemaType, "schema": definition, "references": references,
		}, http.StatusOK)
	}
	putTargetJSON(t, ctx, target, "/config/"+subject, map[string]any{"compatibility": testCase.Level}, http.StatusOK)
	if strings.HasSuffix(testCase.Level, "_TRANSITIVE") {
		status := targetJSONStatus(t, ctx, target, http.MethodPost, "/subjects/"+subject+"/versions", map[string]any{
			"schemaType": schemaType, "schema": testCase.Candidate, "references": references,
		})
		if status != http.StatusOK && status != http.StatusConflict {
			t.Fatalf("%s transitive registration status = %d", target.Name(), status)
		}
		return status == http.StatusOK
	}
	response := postTargetJSON(t, ctx, target, "/compatibility/subjects/"+subject+"/versions/latest?verbose=true", map[string]any{
		"schemaType": schemaType, "schema": testCase.Candidate, "references": references,
	}, http.StatusOK)
	decision, ok := response["is_compatible"].(bool)
	if !ok {
		t.Fatalf("%s returned no compatibility decision: %v", target.Name(), response)
	}
	return decision
}

func targetJSONStatus(t testing.TB, ctx context.Context, target contract.Target, method, path string, body any) int {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, method, target.BaseURL().String()+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := target.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func putTargetJSON(t testing.TB, ctx context.Context, target contract.Target, path string, body any, status int) map[string]any {
	t.Helper()
	return targetJSON(t, ctx, target, http.MethodPut, path, body, status)
}

func postTargetJSON(t testing.TB, ctx context.Context, target contract.Target, path string, body any, status int) map[string]any {
	t.Helper()
	return targetJSON(t, ctx, target, http.MethodPost, path, body, status)
}

func targetJSON(t testing.TB, ctx context.Context, target contract.Target, method, path string, body any, status int) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, method, target.BaseURL().String()+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := target.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("%s %s %s status = %d: %s", target.Name(), method, path, response.StatusCode, responseBody)
	}
	var decoded map[string]any
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		t.Fatalf("decode %s response: %v: %s", target.Name(), err, responseBody)
	}
	return decoded
}

func closeTarget(t testing.TB, target contract.Target) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := target.Close(ctx); err != nil {
		t.Error(err)
	}
}
