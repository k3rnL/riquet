package helm_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestChartLintAndTemplateSnapshots(t *testing.T) {
	chart := chartPath(t)
	run(t, "helm", "lint", chart)
	pvc := run(t, "helm", "template", "snapshot", chart)
	kafka := run(t, "helm", "template", "snapshot", chart,
		"--values", filepath.Join(chart, "../../test/helm/testdata/kafka-values.yaml"))
	assertHash(t, pvc, "0738a1450190f0457c47cbac674e9fbdc2649c1193e4f9508531b146512509c5")
	assertHash(t, kafka, "a785b35639bb203de6cecb7c8010d68a4d523888c2147555b2b582d5ac33f97a")
	for _, expected := range []string{"kind: StatefulSet", "startupProbe:", "readinessProbe:", "runAsNonRoot: true", "volumeClaimTemplates:"} {
		if !bytes.Contains(pvc, []byte(expected)) {
			t.Fatalf("PVC template missing %q", expected)
		}
	}
	for _, expected := range []string{"kind: PodDisruptionBudget", "RIQUET_INTERNAL_TOKEN", "RIQUET_STORAGE_BACKEND, value: \"kafka\"", "replicas: 3", "image: \"registry.example/riquet:1.0.2\"", "emptyDir: {}"} {
		if !bytes.Contains(kafka, []byte(expected)) {
			t.Fatalf("Kafka template missing %q", expected)
		}
	}
	for _, unexpected := range []string{"volumeClaimTemplates:", "RIQUET_DATA_PATH", "name: data"} {
		if bytes.Contains(kafka, []byte(unexpected)) {
			t.Fatalf("Kafka template unexpectedly contains %q", unexpected)
		}
	}
}

func TestKafkaInternalTokenSecret(t *testing.T) {
	chart := chartPath(t)
	kafkaArguments := []string{
		"template", "generated", chart,
		"--set", "storage.backend=kafka",
		"--set", "replicaCount=3",
		"--set", "storage.kafka.brokers[0]=kafka:9092",
	}
	generated := run(t, "helm", kafkaArguments...)
	for _, expected := range []string{
		"kind: Secret",
		"name: generated-riquet-internal",
		`secretKeyRef: {name: "generated-riquet-internal", key: "internal-token"}`,
	} {
		if !bytes.Contains(generated, []byte(expected)) {
			t.Fatalf("generated-secret template missing %q", expected)
		}
	}
	match := regexp.MustCompile(`(?m)^  internal-token: "([^"]+)"$`).FindSubmatch(generated)
	if len(match) != 2 {
		t.Fatal("generated-secret template does not contain the internal token data key")
	}
	token, err := base64.StdEncoding.DecodeString(string(match[1]))
	if err != nil {
		t.Fatalf("decode generated internal token: %v", err)
	}
	if len(token) != 64 {
		t.Fatalf("generated internal token length = %d, want 64", len(token))
	}

	external := run(t, "helm", append(kafkaArguments,
		"--set", "auth.internalTokenSecret.name=existing-internal")...)
	if bytes.Contains(external, []byte("kind: Secret")) {
		t.Fatal("external-secret mode unexpectedly rendered a Secret")
	}
	if !bytes.Contains(external, []byte(`secretKeyRef: {name: "existing-internal", key: "internal-token"}`)) {
		t.Fatal("external-secret mode does not reference the configured Secret")
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
