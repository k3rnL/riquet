// Package kafka implements the Kafka-backed multi-replica storage profile.
package kafka

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

const (
	DefaultTopic              = "_riquet_state"
	DefaultTransactionTimeout = 30 * time.Second
	DefaultOperationTimeout   = 30 * time.Second
)

// Options configures one registry state stream. Credentials are deliberately
// represented by franz-go mechanisms so this package never renders them.
type Options struct {
	Brokers            []string
	Topic              string
	ClientID           string
	TransactionalID    string
	ReplicationFactor  int16
	AutoCreateTopic    bool
	TLS                *tls.Config
	SASL               []sasl.Mechanism
	TransactionTimeout time.Duration
	OperationTimeout   time.Duration
	RequireLeadership  bool
	MaxReadyLag        int64
}

func (o Options) normalized() (Options, error) {
	if len(o.Brokers) == 0 {
		return Options{}, errors.New("at least one Kafka broker is required")
	}
	for _, broker := range o.Brokers {
		if strings.TrimSpace(broker) == "" {
			return Options{}, errors.New("kafka broker address cannot be empty")
		}
	}
	if o.Topic == "" {
		o.Topic = DefaultTopic
	}
	if o.ClientID == "" {
		o.ClientID = "riquet"
	}
	if o.TransactionalID == "" {
		o.TransactionalID = "riquet-" + o.Topic
	}
	if o.ReplicationFactor == 0 {
		o.ReplicationFactor = 1
	}
	if o.ReplicationFactor < 1 {
		return Options{}, fmt.Errorf("kafka replication factor must be positive")
	}
	if o.TransactionTimeout <= 0 {
		o.TransactionTimeout = DefaultTransactionTimeout
	}
	if o.OperationTimeout <= 0 {
		o.OperationTimeout = DefaultOperationTimeout
	}
	if o.MaxReadyLag < 0 {
		return Options{}, errors.New("maximum ready lag cannot be negative")
	}
	if o.TLS != nil {
		o.TLS = o.TLS.Clone()
	}
	return o, nil
}

func commonClientOptions(options Options) []kgo.Opt {
	result := []kgo.Opt{
		kgo.SeedBrokers(options.Brokers...),
		kgo.ClientID(options.ClientID),
		kgo.MetadataMaxAge(10 * time.Second),
	}
	if options.TLS != nil {
		result = append(result, kgo.DialTLSConfig(options.TLS))
	}
	if len(options.SASL) > 0 {
		result = append(result, kgo.SASL(options.SASL...))
	}
	return result
}
