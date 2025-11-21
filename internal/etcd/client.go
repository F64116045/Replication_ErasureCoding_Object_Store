package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	// _client is the Singleton instance
	_client *clientv3.Client
	// once ensures initialization runs only once
	once sync.Once
)

// GetMetadata retrieves "metadata/<key>" from etcd and unmarshals it into a map.
// Note: This is a helper function. In the layered architecture, services might call client.Get() directly via interfaces.
func GetMetadata(ctx context.Context, key string) (map[string]interface{}, error) {
	client := GetClient()
	fullKey := fmt.Sprintf("metadata/%s", key)

	resp, err := client.Get(ctx, fullKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read from etcd: %v", err)
	}

	// Return empty map if not found (convenience for hotOnly checks)
	if len(resp.Kvs) == 0 {
		return map[string]interface{}{}, nil
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal(resp.Kvs[0].Value, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata json: %v", err)
	}

	return metadata, nil
}

// GetClient returns the singleton Etcd client.
// It initializes the connection on the first call.
func GetClient() *clientv3.Client {
	once.Do(func() {
		// 1. Load configuration from environment variables
		etcdHost := os.Getenv("ETCD_HOST")
		etcdPort := os.Getenv("ETCD_PORT")
		if etcdHost == "" {
			etcdHost = "localhost"
		}
		if etcdPort == "" {
			etcdPort = "2379"
		}

		etcdEndpoint := etcdHost + ":" + etcdPort
		log.Printf("[ETCD Client] Connecting to etcd: %s", etcdEndpoint)

		// 2. Create official Go client with timeout
		// DialTimeout helps handle startup race conditions in Docker Compose
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{etcdEndpoint},
			DialTimeout: 5 * time.Second,
		})

		if err != nil {
			log.Fatalf("[ETCD Client] Critical Error: Failed to connect to etcd: %v", err)
		}

		_client = cli
		log.Println("[ETCD Client] Connection successful.")
	})

	return _client
}

// CloseClient gracefully closes the etcd connection.
func CloseClient() {
	if _client != nil {
		log.Println("[ETCD Client] Closing connection...")
		_client.Close()
	}
}