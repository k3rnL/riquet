package helm_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestChartLintAndTemplateSnapshots(t *testing.T) {
	chart := chartPath(t)
	run(t, "helm", "lint", chart)
	pvc := run(t, "helm", "template", "snapshot", chart)
	kafka := run(t, "helm", "template", "snapshot", chart,
		"--set", "storage.backend=kafka", "--set", "replicaCount=3",
		"--set", "storage.kafka.brokers[0]=kafka:9092", "--set", "auth.internalTokenSecret.name=riquet-internal")
	assertHash(t, pvc, "fde2adc57c36d7c147f5f53ec4a911ccd33ab486d795395dc133e8814a41fc93")
	assertHash(t, kafka, "ccfa833fb38944fec6d48fb6761876904e56f9d8acd397e5de52b21528b1f8ca")
	for _, expected := range []string{"kind: StatefulSet", "startupProbe:", "readinessProbe:", "runAsNonRoot: true", "volumeClaimTemplates:"} {
		if !bytes.Contains(pvc, []byte(expected)) {
			t.Fatalf("PVC template missing %q", expected)
		}
	}
	for _, expected := range []string{"kind: PodDisruptionBudget", "RIQUET_INTERNAL_TOKEN", "replicas: 3", "emptyDir: {}"} {
		if !bytes.Contains(kafka, []byte(expected)) {
			t.Fatalf("Kafka template missing %q", expected)
		}
	}
}

func TestValuesSchemaRejectsInvalidHAProfiles(t *testing.T) {
	chart := chartPath(t)
	assertHelmFails(t, chart, "--set", "replicaCount=2")
	assertHelmFails(t, chart, "--set", "storage.backend=kafka", "--set", "replicaCount=2")
	assertHelmFails(t, chart, "--set", "storage.backend=kafka", "--set", "replicaCount=1",
		"--set", "storage.kafka.brokers[0]=kafka:9092", "--set", "auth.internalTokenSecret.name=token")
}

func TestRenderedKubernetesSchemas(t *testing.T) {
	if _, err := exec.LookPath("kubeconform"); err != nil {
		t.Skip("kubeconform is not installed")
	}
	chart := chartPath(t)
	rendered := run(t, "helm", "template", "schema", chart)
	command := exec.Command("kubeconform", "-strict", "-summary")
	command.Stdin = bytes.NewReader(rendered)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("kubeconform: %v: %s", err, output)
	}
}

func chartPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs("../../charts/riquet")
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func run(t *testing.T, name string, arguments ...string) []byte {
	t.Helper()
	command := exec.Command(name, arguments...)
	command.Env = append(os.Environ(), "HELM_PLUGINS=/nonexistent")
	output, err := command.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			t.Fatalf("%s %s: %v: %s", name, strings.Join(arguments, " "), err, exit.Stderr)
		}
		t.Fatalf("%s: %v", name, err)
	}
	return output
}

func assertHash(t *testing.T, value []byte, expected string) {
	t.Helper()
	sum := sha256.Sum256(value)
	got := hex.EncodeToString(sum[:])
	if got != expected {
		t.Fatalf("template snapshot hash = %s, want %s", got, expected)
	}
}

func assertHelmFails(t *testing.T, chart string, arguments ...string) {
	t.Helper()
	base := []string{"template", "invalid", chart}
	command := exec.Command("helm", append(base, arguments...)...)
	command.Env = append(os.Environ(), "HELM_PLUGINS=/nonexistent")
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("invalid Helm values accepted: %s", output)
	}
}
