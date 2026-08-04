package kafka

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/domain"
)

func TestOptionsNormalizeSafeDefaultsAndCloneTLS(t *testing.T) {
	tlsConfig := &tls.Config{ServerName: "broker.example", MinVersion: tls.VersionTLS12}
	options, err := (Options{Brokers: []string{"broker:9092"}, TLS: tlsConfig}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	if options.Topic != DefaultTopic || options.TransactionalID != "riquet-"+DefaultTopic {
		t.Fatalf("unexpected identity defaults: %+v", options)
	}
	if options.TransactionTimeout != DefaultTransactionTimeout || options.OperationTimeout != DefaultOperationTimeout {
		t.Fatalf("unexpected timeout defaults: %+v", options)
	}
	if options.TLS == tlsConfig || options.TLS.ServerName != tlsConfig.ServerName {
		t.Fatal("TLS configuration was not defensively cloned")
	}
	options.TLS.ServerName = "changed"
	if tlsConfig.ServerName != "broker.example" {
		t.Fatal("normalization mutated caller TLS configuration")
	}
}

func TestOptionsRejectInvalidConnectionAndReplication(t *testing.T) {
	tests := []Options{
		{},
		{Brokers: []string{""}},
		{Brokers: []string{"broker:9092"}, ReplicationFactor: -1},
	}
	for _, options := range tests {
		if _, err := options.normalized(); err == nil {
			t.Fatalf("invalid options accepted: %+v", options)
		}
	}
}

func TestExplicitTimeoutsSurviveNormalization(t *testing.T) {
	options, err := (Options{
		Brokers: []string{"broker:9092"}, TransactionTimeout: 12 * time.Second,
		OperationTimeout: 7 * time.Second,
	}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	if options.TransactionTimeout != 12*time.Second || options.OperationTimeout != 7*time.Second {
		t.Fatalf("explicit timeouts changed: %+v", options)
	}
}

func TestVersionedRecordKeysRoundTrip(t *testing.T) {
	for _, sequence := range []uint64{1, 9, 10, 999, 1<<63 + 7} {
		key := transitionKey(domain.Sequence(sequence))
		decoded, ok := parseTransitionKey(key)
		if !ok || uint64(decoded) != sequence {
			t.Fatalf("parseTransitionKey(%q) = %d, %t", key, decoded, ok)
		}
	}
	invalid := []string{"", "v2/transition/00000000000000000001", transitionPrefix + "1", transitionPrefix + "00000000000000000000"}
	for _, key := range invalid {
		if _, ok := parseTransitionKey(key); ok {
			t.Fatalf("invalid key %q accepted", key)
		}
	}
}

func TestStatusRemovesLaggingFollowerFromReadiness(t *testing.T) {
	store := &Store{
		options: Options{MaxReadyLag: 2}, ready: true, items: make(map[domain.Sequence][]byte),
		position: 7, end: 10,
	}
	status := store.Status()
	if status.Ready || status.Role != "follower" || status.Lag != 3 {
		t.Fatalf("lagging status = %+v", status)
	}
	store.position = 8
	status = store.Status()
	if !status.Ready || status.Lag != 2 {
		t.Fatalf("caught-up status = %+v", status)
	}
}
