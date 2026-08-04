package e2e_test

import (
	"path/filepath"
	"testing"

	"github.com/k3rnL/riquet/internal/contract"
)

func TestDiscoveryScenarioFilesLoad(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("scenarios/discovery/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no discovery scenarios found")
	}
	for _, file := range files {
		file := file
		t.Run(filepath.Base(file), func(t *testing.T) {
			t.Parallel()
			if _, err := contract.LoadScenario(file); err != nil {
				t.Fatal(err)
			}
		})
	}
}
