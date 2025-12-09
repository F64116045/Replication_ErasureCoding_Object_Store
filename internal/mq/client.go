package mq

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm" // Kafka Admin 套件
	"github.com/twmb/franz-go/pkg/kgo"
)

type Client struct {
	client *kgo.Client
	topic  string
}

// NewClient initializes a connection to Redpanda and ensures the Topic exists.
func NewClient() *Client {
	broker := os.Getenv("WAL_BROKER")
	topic := os.Getenv("WAL_TOPIC")

	if broker == "" {
		broker = "redpanda:9092"
	}
	if topic == "" {
		topic = "wal-events"
	}

	log.Printf("[Redpanda] Connecting to broker: %s", broker)

	// 1. Initialize Client
	opts := []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("[Redpanda] Failed to create client: %v", err)
	}

	// 2. Health Check (Ping)
	// Retry logic for connection (Wait for Redpanda to be ready)
	for i := 0; i < 30; i++ { // Retry for 60 seconds
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = cl.Ping(ctx)
		cancel()
		if err == nil {
			break
		}
		log.Printf("[Redpanda] Waiting for broker... (%d/30)", i+1)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("[Redpanda] Connection timeout. Is Redpanda running?")
	}

	// 3. Auto-Create Topic using Admin API
	// This replaces the ugly init-container shell script.
	ensureTopicExists(cl, topic)

	log.Println("[Redpanda] Connection established & Topic verified.")
	return &Client{client: cl, topic: topic}
}

func (c *Client) Close() {
	c.client.Close()
}

func (c *Client) ProduceSync(ctx context.Context, key string, value []byte) error {
	record := &kgo.Record{
		Key:   []byte(key),
		Value: value,
	}
	if err := c.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("failed to produce WAL entry: %w", err)
	}
	return nil
}

// ensureTopicExists uses the Admin API to create the topic if it's missing.
func ensureTopicExists(cl *kgo.Client, topic string) {
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Fetch metadata to check if topic exists
	topicsMetadata, err := adm.ListTopics(ctx, topic)
	if err == nil && topicsMetadata.Has(topic) {
		// Topic exists, we are good.
		return
	}

	log.Printf("[Redpanda] Topic '%s' not found. Creating...", topic)

	// Create Topic: 3 Partitions, 1 Replica
	resp, err := adm.CreateTopics(ctx, 3, 1, nil, topic)
	if err != nil {
		// Ignore "TopicAlreadyExists" error (concurrency safety)
		log.Printf("[Redpanda] Warning during creation: %v", err)
		return
	}

	// Check response for specific errors
	for _, t := range resp {
		if t.Err != nil {
			log.Printf("[Redpanda] Topic creation result for %s: %v", t.Topic, t.Err)
		} else {
			log.Printf("[Redpanda] Topic '%s' created successfully.", t.Topic)
		}
	}
}