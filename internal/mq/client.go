package mq

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm" // 需要這個 Admin 套件
	"github.com/twmb/franz-go/pkg/kgo"
)

type Client struct {
	client *kgo.Client
	topic  string
}

// NewClient initializes a connection to Redpanda AND ensures the topic exists.
func NewClient(isConsumer bool) *Client {
	broker := os.Getenv("WAL_BROKER")
	topic := os.Getenv("WAL_TOPIC")

	if broker == "" {
		broker = "redpanda:9092"
	}
	if topic == "" {
		topic = "wal-events"
	}

	log.Printf("[Redpanda] Connecting to broker: %s, topic: %s", broker, topic)

	// 1. Configure Options
	opts := []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	}

	if isConsumer {
		opts = append(opts, 
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup("healer-group"),
			kgo.DisableAutoCommit(),
		)
	} else {
		opts = append(opts, kgo.DefaultProduceTopic(topic))
	}

	// 2. Initialize Client
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("[Redpanda] Failed to create client: %v", err)
	}

	// 3. Health Check (Ping) & Retry
	// Redpanda takes time to start, so we retry a few times.
	connected := false
	for i := 0; i < 15; i++ { 
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := cl.Ping(ctx); err == nil {
			connected = true
			cancel()
			break
		}
		cancel()
		log.Printf("[Redpanda] Waiting for broker... (%d/15)", i+1)
		time.Sleep(2 * time.Second)
	}

	if !connected {
		log.Fatalf("[Redpanda] Connection timeout. Is Redpanda running?")
	}

	// 4. [Auto-Create Topic] Only Producer needs to care about this strictly,
	// but it doesn't hurt for Consumer to check too.
	if !isConsumer {
		ensureTopicExists(cl, topic)
	}

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

func (c *Client) Consume(ctx context.Context, handler func(key, value []byte) error) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Printf("[Redpanda] Consume error: %v", errs)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			time.Sleep(1 * time.Second)
			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			if err := handler(record.Key, record.Value); err != nil {
				log.Printf("[Redpanda] Handler failed: %v", err)
				continue 
			}
			c.client.CommitRecords(ctx, record)
		}
	}
}

// ensureTopicExists uses the Admin API to create the topic if it's missing.
func ensureTopicExists(cl *kgo.Client, topic string) {
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// List topics to check existence
	topicsMetadata, err := adm.ListTopics(ctx, topic)
	if err == nil && topicsMetadata.Has(topic) {
		return // Topic exists
	}

	log.Printf("[Redpanda] Topic '%s' not found. Creating...", topic)

	// Create Topic: 3 Partitions, 1 Replica
	// Note: In single-node Redpanda, Replication Factor must be 1.
	resp, err := adm.CreateTopics(ctx, 3, 1, nil, topic)
	if err != nil {
		log.Printf("[Redpanda] Warning during creation: %v", err)
		return
	}

	for _, t := range resp {
		if t.Err != nil {
			log.Printf("[Redpanda] Create topic result: %v", t.Err)
		} else {
			log.Printf("[Redpanda] Topic '%s' created successfully.", t.Topic)
		}
	}
}