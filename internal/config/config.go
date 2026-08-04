// Package config loads and validates secret-safe runtime configuration.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listeners Listeners `yaml:"listeners" json:"listeners"`
	Storage   Storage   `yaml:"storage" json:"storage"`
	TLS       TLS       `yaml:"tls" json:"tls"`
	Auth      Auth      `yaml:"auth" json:"auth"`
	Limits    Limits    `yaml:"limits" json:"limits"`
}

type Listeners struct {
	Public   string `yaml:"public" json:"public"`
	Internal string `yaml:"internal" json:"internal"`
	Health   string `yaml:"health" json:"health"`
	Metrics  string `yaml:"metrics" json:"metrics"`
}

type Storage struct {
	Backend string       `yaml:"backend" json:"backend"`
	PVC     PVCStorage   `yaml:"pvc" json:"pvc"`
	Kafka   KafkaStorage `yaml:"kafka" json:"kafka"`
}

type PVCStorage struct {
	Path string `yaml:"path" json:"path"`
}

type KafkaStorage struct {
	Brokers            []string      `yaml:"brokers" json:"brokers"`
	Topic              string        `yaml:"topic" json:"topic"`
	TransactionalID    string        `yaml:"transactionalId" json:"transactionalId"`
	GroupID            string        `yaml:"groupId" json:"groupId"`
	ReplicaID          string        `yaml:"replicaId" json:"replicaId"`
	AdvertiseURL       string        `yaml:"advertiseUrl" json:"advertiseUrl"`
	ReplicationFactor  int16         `yaml:"replicationFactor" json:"replicationFactor"`
	AutoCreateTopic    bool          `yaml:"autoCreateTopic" json:"autoCreateTopic"`
	MaxReadyLag        int64         `yaml:"maxReadyLag" json:"maxReadyLag"`
	TransactionTimeout time.Duration `yaml:"transactionTimeout" json:"transactionTimeout"`
	TLS                KafkaTLS      `yaml:"tls" json:"tls"`
	SASL               KafkaSASL     `yaml:"sasl" json:"sasl"`
}

type KafkaTLS struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	CAFile     string `yaml:"caFile" json:"caFile"`
	CertFile   string `yaml:"certFile" json:"certFile"`
	KeyFile    string `yaml:"keyFile" json:"keyFile"`
	ServerName string `yaml:"serverName" json:"serverName"`
}

type KafkaSASL struct {
	Mechanism string `yaml:"mechanism" json:"mechanism"`
	Username  string `yaml:"username" json:"username"`
	Password  string `yaml:"password" json:"-"`
}

type TLS struct {
	CertFile          string   `yaml:"certFile" json:"certFile"`
	KeyFile           string   `yaml:"keyFile" json:"keyFile"`
	TrustedProxyCIDRs []string `yaml:"trustedProxyCidrs" json:"trustedProxyCidrs"`
}

type Auth struct {
	Mode          string `yaml:"mode" json:"mode"`
	Username      string `yaml:"username" json:"username"`
	Password      string `yaml:"password" json:"-"`
	BearerToken   string `yaml:"bearerToken" json:"-"`
	InternalToken string `yaml:"internalToken" json:"-"`
	AdminToken    string `yaml:"adminToken" json:"-"`
}

type Limits struct {
	RequestBytes    int64         `yaml:"requestBytes" json:"requestBytes"`
	ReadTimeout     time.Duration `yaml:"readTimeout" json:"readTimeout"`
	WriteTimeout    time.Duration `yaml:"writeTimeout" json:"writeTimeout"`
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout" json:"shutdownTimeout"`
}

// String renders redacted JSON suitable for startup diagnostics.
func (c Config) String() string {
	raw, err := json.Marshal(c)
	if err != nil {
		return `{"configuration":"unavailable"}`
	}
	return string(raw)
}

// Overrides represents explicitly set command-line values.
type Overrides struct {
	PublicAddress *string
	DataPath      *string
	Backend       *string
}

func Default() Config {
	return Config{
		Listeners: Listeners{Public: ":8081", Health: ":8082", Metrics: ":9090"},
		Storage: Storage{Backend: "pvc", PVC: PVCStorage{Path: ".riquet/riquet.db"}, Kafka: KafkaStorage{
			Topic: "_riquet_state", ReplicationFactor: 1, TransactionTimeout: 30 * time.Second,
		}},
		Auth:   Auth{Mode: "anonymous"},
		Limits: Limits{RequestBytes: 2 << 20, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, ShutdownTimeout: 15 * time.Second},
	}
}

// Load applies defaults, optional strict YAML, RIQUET_* environment values,
// and explicit flags in that order.
func Load(path string, environment map[string]string, overrides Overrides) (Config, error) {
	result := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read configuration: %w", err)
		}
		decoder := yaml.NewDecoder(bytes.NewReader(raw))
		decoder.KnownFields(true)
		if err := decoder.Decode(&result); err != nil {
			return Config{}, fmt.Errorf("decode configuration: %w", err)
		}
	}
	if environment == nil {
		environment = currentEnvironment()
	}
	if err := applyEnvironment(&result, environment); err != nil {
		return Config{}, err
	}
	if overrides.PublicAddress != nil {
		result.Listeners.Public = *overrides.PublicAddress
	}
	if overrides.DataPath != nil {
		result.Storage.PVC.Path = *overrides.DataPath
	}
	if overrides.Backend != nil {
		result.Storage.Backend = *overrides.Backend
	}
	if err := result.Validate(); err != nil {
		return Config{}, err
	}
	return result, nil
}

func (c Config) Validate() error {
	if c.Listeners.Public == "" {
		return errors.New("public listener is required")
	}
	seen := make(map[string]string)
	for name, address := range map[string]string{"public": c.Listeners.Public, "internal": c.Listeners.Internal, "health": c.Listeners.Health, "metrics": c.Listeners.Metrics} {
		if address == "" {
			continue
		}
		if previous, exists := seen[address]; exists {
			return fmt.Errorf("%s and %s listeners cannot share %q", previous, name, address)
		}
		seen[address] = name
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		return errors.New("TLS certificate and key must be configured together")
	}
	switch c.Auth.Mode {
	case "anonymous":
	case "basic":
		if c.Auth.Username == "" || c.Auth.Password == "" {
			return errors.New("basic authentication requires username and password")
		}
	case "bearer":
		if c.Auth.BearerToken == "" {
			return errors.New("bearer authentication requires a token")
		}
	default:
		return fmt.Errorf("unsupported authentication mode %q", c.Auth.Mode)
	}
	switch c.Storage.Backend {
	case "pvc":
		if c.Storage.PVC.Path == "" {
			return errors.New("PVC storage path is required")
		}
	case "kafka":
		if len(c.Storage.Kafka.Brokers) == 0 || c.Storage.Kafka.Topic == "" {
			return errors.New("kafka brokers and topic are required")
		}
		if c.Listeners.Internal == "" || c.Auth.InternalToken == "" || c.Storage.Kafka.ReplicaID == "" || c.Storage.Kafka.AdvertiseURL == "" {
			return errors.New("kafka HA requires an internal listener, token, replica ID, and advertise URL")
		}
		if (c.Storage.Kafka.TLS.CertFile == "") != (c.Storage.Kafka.TLS.KeyFile == "") {
			return errors.New("kafka TLS client certificate and key must be configured together")
		}
		switch c.Storage.Kafka.SASL.Mechanism {
		case "":
		case "plain", "scram-sha-256", "scram-sha-512":
			if c.Storage.Kafka.SASL.Username == "" || c.Storage.Kafka.SASL.Password == "" {
				return errors.New("kafka SASL requires username and password")
			}
		default:
			return fmt.Errorf("unsupported Kafka SASL mechanism %q", c.Storage.Kafka.SASL.Mechanism)
		}
	default:
		return fmt.Errorf("unsupported storage backend %q", c.Storage.Backend)
	}
	if c.Limits.RequestBytes < 1 || c.Limits.ReadTimeout <= 0 || c.Limits.WriteTimeout <= 0 || c.Limits.ShutdownTimeout <= 0 {
		return errors.New("request and timeout limits must be positive")
	}
	return nil
}

func applyEnvironment(config *Config, values map[string]string) error {
	set := func(key string, target *string) {
		if value, ok := values[key]; ok {
			*target = value
		}
	}
	set("RIQUET_PUBLIC_ADDRESS", &config.Listeners.Public)
	set("RIQUET_INTERNAL_ADDRESS", &config.Listeners.Internal)
	set("RIQUET_HEALTH_ADDRESS", &config.Listeners.Health)
	set("RIQUET_METRICS_ADDRESS", &config.Listeners.Metrics)
	set("RIQUET_STORAGE_BACKEND", &config.Storage.Backend)
	set("RIQUET_DATA_PATH", &config.Storage.PVC.Path)
	set("RIQUET_KAFKA_TOPIC", &config.Storage.Kafka.Topic)
	set("RIQUET_KAFKA_TRANSACTIONAL_ID", &config.Storage.Kafka.TransactionalID)
	set("RIQUET_KAFKA_GROUP_ID", &config.Storage.Kafka.GroupID)
	set("RIQUET_REPLICA_ID", &config.Storage.Kafka.ReplicaID)
	set("RIQUET_INTERNAL_ADVERTISE_URL", &config.Storage.Kafka.AdvertiseURL)
	set("RIQUET_KAFKA_TLS_CA_FILE", &config.Storage.Kafka.TLS.CAFile)
	set("RIQUET_KAFKA_TLS_CERT_FILE", &config.Storage.Kafka.TLS.CertFile)
	set("RIQUET_KAFKA_TLS_KEY_FILE", &config.Storage.Kafka.TLS.KeyFile)
	set("RIQUET_KAFKA_TLS_SERVER_NAME", &config.Storage.Kafka.TLS.ServerName)
	set("RIQUET_KAFKA_SASL_MECHANISM", &config.Storage.Kafka.SASL.Mechanism)
	set("RIQUET_KAFKA_SASL_USERNAME", &config.Storage.Kafka.SASL.Username)
	set("RIQUET_KAFKA_SASL_PASSWORD", &config.Storage.Kafka.SASL.Password)
	if value, ok := values["RIQUET_KAFKA_BROKERS"]; ok {
		config.Storage.Kafka.Brokers = splitNonEmpty(value)
	}
	set("RIQUET_TLS_CERT_FILE", &config.TLS.CertFile)
	set("RIQUET_TLS_KEY_FILE", &config.TLS.KeyFile)
	if value, ok := values["RIQUET_TRUSTED_PROXY_CIDRS"]; ok {
		config.TLS.TrustedProxyCIDRs = splitNonEmpty(value)
	}
	set("RIQUET_AUTH_MODE", &config.Auth.Mode)
	set("RIQUET_AUTH_USERNAME", &config.Auth.Username)
	set("RIQUET_AUTH_PASSWORD", &config.Auth.Password)
	set("RIQUET_AUTH_BEARER_TOKEN", &config.Auth.BearerToken)
	set("RIQUET_INTERNAL_TOKEN", &config.Auth.InternalToken)
	set("RIQUET_ADMIN_TOKEN", &config.Auth.AdminToken)
	if value, ok := values["RIQUET_MAX_READY_LAG"]; ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("RIQUET_MAX_READY_LAG: %w", err)
		}
		config.Storage.Kafka.MaxReadyLag = parsed
	}
	if value, ok := values["RIQUET_KAFKA_REPLICATION_FACTOR"]; ok {
		parsed, err := strconv.ParseInt(value, 10, 16)
		if err != nil || parsed < 1 {
			return fmt.Errorf("RIQUET_KAFKA_REPLICATION_FACTOR: invalid positive int16")
		}
		config.Storage.Kafka.ReplicationFactor = int16(parsed)
	}
	if value, ok := values["RIQUET_KAFKA_AUTO_CREATE_TOPIC"]; ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("RIQUET_KAFKA_AUTO_CREATE_TOPIC: %w", err)
		}
		config.Storage.Kafka.AutoCreateTopic = parsed
	}
	if value, ok := values["RIQUET_KAFKA_TLS_ENABLED"]; ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("RIQUET_KAFKA_TLS_ENABLED: %w", err)
		}
		config.Storage.Kafka.TLS.Enabled = parsed
	}
	return nil
}

func currentEnvironment() map[string]string {
	result := make(map[string]string)
	for _, item := range os.Environ() {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			result[key] = value
		}
	}
	return result
}

func splitNonEmpty(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}
