package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const cleanupPolicyCompact = "compact"

func provisionAndValidateTopic(ctx context.Context, client *kgo.Client, options Options) error {
	if options.AutoCreateTopic {
		if err := createTopic(ctx, client, options); err != nil {
			return err
		}
	}
	metadata := kmsg.NewPtrMetadataRequest()
	topic := kmsg.NewMetadataRequestTopic()
	topic.Topic = kmsg.StringPtr(options.Topic)
	metadata.Topics = []kmsg.MetadataRequestTopic{topic}
	metadata.AllowAutoTopicCreation = false
	response, err := metadata.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("read Kafka state topic metadata: %w", err)
	}
	if len(response.Topics) != 1 {
		return fmt.Errorf("kafka state topic %q metadata was not returned", options.Topic)
	}
	if err := kerr.ErrorForCode(response.Topics[0].ErrorCode); err != nil {
		return fmt.Errorf("kafka state topic %q: %w", options.Topic, err)
	}
	if len(response.Topics[0].Partitions) != 1 || response.Topics[0].Partitions[0].Partition != topicPartition {
		return fmt.Errorf("kafka state topic %q must have exactly one partition", options.Topic)
	}
	if err := kerr.ErrorForCode(response.Topics[0].Partitions[0].ErrorCode); err != nil {
		return fmt.Errorf("kafka state topic %q partition: %w", options.Topic, err)
	}
	return validateTopicConfigs(ctx, client, options.Topic)
}

func createTopic(ctx context.Context, client *kgo.Client, options Options) error {
	compact := cleanupPolicyCompact
	retention := "-1"
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = options.Topic
	topic.NumPartitions = 1
	topic.ReplicationFactor = options.ReplicationFactor
	topic.Configs = []kmsg.CreateTopicsRequestTopicConfig{
		{Name: "cleanup.policy", Value: &compact},
		{Name: "retention.ms", Value: &retention},
	}
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = int32((30 * time.Second) / time.Millisecond)
	request.Topics = []kmsg.CreateTopicsRequestTopic{topic}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("create Kafka state topic: %w", err)
	}
	if len(response.Topics) != 1 {
		return errors.New("kafka returned no state-topic creation result")
	}
	if err := kerr.ErrorForCode(response.Topics[0].ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create Kafka state topic %q: %w", options.Topic, err)
	}
	return nil
}

func validateTopicConfigs(ctx context.Context, client *kgo.Client, topic string) error {
	request := kmsg.NewPtrDescribeConfigsRequest()
	request.Resources = []kmsg.DescribeConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: topic,
		ConfigNames:  []string{"cleanup.policy", "retention.ms"},
	}}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("describe Kafka state topic configuration: %w", err)
	}
	if len(response.Resources) != 1 {
		return errors.New("kafka returned no state-topic configuration")
	}
	resource := response.Resources[0]
	if err := kerr.ErrorForCode(resource.ErrorCode); err != nil {
		return fmt.Errorf("describe Kafka state topic %q: %w", topic, err)
	}
	configs := make(map[string]string, len(resource.Configs))
	for _, config := range resource.Configs {
		if config.Value != nil {
			configs[config.Name] = *config.Value
		}
	}
	if configs["cleanup.policy"] != cleanupPolicyCompact {
		return fmt.Errorf("kafka state topic %q cleanup.policy must be compact, got %q", topic, configs["cleanup.policy"])
	}
	if configs["retention.ms"] != "-1" {
		return fmt.Errorf("kafka state topic %q retention.ms must be -1, got %q", topic, configs["retention.ms"])
	}
	return nil
}
