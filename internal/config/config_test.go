package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrecedenceDefaultsYAMLEnvironmentFlags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "riquet.yaml")
	if err := os.WriteFile(path, []byte(`
listeners:
  public: ":7000"
  health: ":7001"
  metrics: ":7002"
storage:
  backend: pvc
  pvc: {path: yaml.db}
auth: {mode: anonymous}
limits:
  requestBytes: 1024
  readTimeout: 2s
  writeTimeout: 3s
  shutdownTimeout: 4s
`), 0o600); err != nil {
		t.Fatal(err)
	}
	flagAddress := ":9000"
	config, err := Load(path, map[string]string{"RIQUET_PUBLIC_ADDRESS": ":8000", "RIQUET_DATA_PATH": "env.db"}, Overrides{PublicAddress: &flagAddress})
	if err != nil {
		t.Fatal(err)
	}
	if config.Listeners.Public != ":9000" || config.Storage.PVC.Path != "env.db" || config.Listeners.Health != ":7001" {
		t.Fatalf("precedence result = %+v", config)
	}
}

func TestStrictYAMLAndInvalidProfiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("unknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, map[string]string{}, Overrides{}); err == nil {
		t.Fatal("unknown YAML field accepted")
	}
	config := Default()
	config.Storage.Backend = "kafka"
	config.Storage.Kafka.Brokers = []string{"broker:9092"}
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "internal") {
		t.Fatalf("invalid Kafka HA error = %v", err)
	}
}

func TestSecretFieldsAreRedactedFromJSON(t *testing.T) {
	config := Default()
	config.Auth.Password = "password-secret"
	config.Auth.BearerToken = "bearer-secret"
	config.Auth.InternalToken = "internal-secret"
	config.Auth.AdminToken = "admin-secret"
	config.Storage.Kafka.SASL.Password = "kafka-secret"
	rendered := config.String()
	for _, secret := range []string{"password-secret", "bearer-secret", "internal-secret", "admin-secret", "kafka-secret"} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("diagnostics exposed %q: %s", secret, rendered)
		}
	}
}

func TestKafkaTLSAndSASLEnvironment(t *testing.T) {
	config, err := Load("", map[string]string{
		"RIQUET_STORAGE_BACKEND": "kafka", "RIQUET_KAFKA_BROKERS": "broker:9093",
		"RIQUET_INTERNAL_ADDRESS": ":8083", "RIQUET_INTERNAL_TOKEN": "token",
		"RIQUET_REPLICA_ID": "replica", "RIQUET_INTERNAL_ADVERTISE_URL": "https://replica:8083",
		"RIQUET_KAFKA_TLS_ENABLED": "true", "RIQUET_KAFKA_TLS_CA_FILE": "/tls/ca.crt",
		"RIQUET_KAFKA_SASL_MECHANISM": "scram-sha-512", "RIQUET_KAFKA_SASL_USERNAME": "user",
		"RIQUET_KAFKA_SASL_PASSWORD": "secret",
	}, Overrides{})
	if err != nil {
		t.Fatal(err)
	}
	if !config.Storage.Kafka.TLS.Enabled || config.Storage.Kafka.SASL.Mechanism != "scram-sha-512" {
		t.Fatalf("Kafka security config = %+v", config.Storage.Kafka)
	}
}
