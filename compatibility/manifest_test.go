package compatibility_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestManifestIsJSONAndPinsOracleDigest(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		ManifestVersion int `json:"manifestVersion"`
		Oracle          struct {
			Version string `json:"version"`
			Digest  string `json:"digest"`
		} `json:"oracle"`
		Brokers []struct {
			Name      string   `json:"name"`
			Version   string   `json:"version"`
			Status    string   `json:"status"`
			Scenarios []string `json:"scenarios"`
		} `json:"brokers"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.ManifestVersion != 1 || manifest.Oracle.Version == "" {
		t.Fatalf("manifest is missing version data: %+v", manifest)
	}
	if len(manifest.Oracle.Digest) != len("sha256:")+64 {
		t.Fatalf("oracle digest is not pinned: %q", manifest.Oracle.Digest)
	}
	if len(manifest.Brokers) == 0 || manifest.Brokers[0].Status != "supported" || len(manifest.Brokers[0].Scenarios) == 0 {
		t.Fatalf("manifest has no executable broker certification: %+v", manifest.Brokers)
	}
}

func TestEverySupportedEndpointLinksToMatchingDifferentialScenario(t *testing.T) {
	raw, err := os.ReadFile("manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		API struct {
			Endpoints []struct {
				Method, Path, Status string
				Scenarios            []string
			} `json:"endpoints"`
		} `json:"api"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	type step struct{ Method, Path string }
	scenarios := make(map[string][]step)
	files, err := filepath.Glob("../test/e2e/scenarios/discovery/*.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		var scenario struct {
			Name  string `json:"name"`
			Steps []step `json:"steps"`
		}
		encoded, readErr := os.ReadFile(file)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if err := json.Unmarshal(encoded, &scenario); err != nil {
			t.Fatal(err)
		}
		scenarios[scenario.Name] = scenario.Steps
	}
	for _, endpoint := range manifest.API.Endpoints {
		if endpoint.Status != "supported" {
			continue
		}
		pattern := regexp.MustCompile("^" + strings.NewReplacer(
			`\{subject\}`, `[^/]+`, `\{id\}`, `[^/]+`, `\{version\}`, `[^/]+`,
		).Replace(regexp.QuoteMeta(endpoint.Path)) + "$")
		matched := false
		for _, scenarioName := range endpoint.Scenarios {
			for _, item := range scenarios[scenarioName] {
				path := strings.SplitN(item.Path, "?", 2)[0]
				if item.Method == endpoint.Method && pattern.MatchString(path) {
					matched = true
				}
			}
		}
		if !matched {
			t.Errorf("%s %s has no matching linked differential scenario", endpoint.Method, endpoint.Path)
		}
	}
}
