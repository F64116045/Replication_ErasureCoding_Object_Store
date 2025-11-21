package storageops

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
)

// Service implements IStorageOps.
type Service struct {
	http interfaces.IHttpClient
}

// NewService creates a new StorageOps service with dependency injection.
func NewService(http interfaces.IHttpClient) interfaces.IStorageOps {
	return &Service{
		http: http,
	}
}

// DeleteReplication deletes the key from all replica nodes concurrently.
func (s *Service) DeleteReplication(ctx context.Context, replicaNodes []string, key string) (int, error) {
	log.Printf("%s[StorageOps] Deleting (Replication) key=%s%s\n", config.Colors["RED"], key, config.Colors["RESET"])

	var wg sync.WaitGroup
	successCount := 0
	var mutex sync.Mutex
	client := s.http

	for _, nodeURL := range replicaNodes {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/delete/%s", url, key), nil)
			if err != nil {
				log.Printf("[%s] Delete failed (req creation error): %v\n", url, err)
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("[%s] Delete failed (network error): %v\n", url, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
				// Count 404 as success (idempotent delete)
				mutex.Lock()
				successCount++
				mutex.Unlock()
			} else {
				log.Printf("[%s] Delete failed (status: %d)\n", url, resp.StatusCode)
			}
		}(nodeURL)
	}

	wg.Wait()
	return successCount, nil
}

// DeleteEC deletes all shards associated with the key from EC nodes concurrently.
func (s *Service) DeleteEC(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (int, error) {
	log.Printf("%s[StorageOps] Deleting (EC) key (metadata: %v)%s\n", config.Colors["RED"], metadata, config.Colors["RESET"])

	var chunkPrefix string
	if prefix, ok := metadata["chunk_prefix"].(string); ok {
		chunkPrefix = prefix
	} else {
		// Fallback for Healer rollback or legacy metadata
		keyName, _ := metadata["key_name"].(string)
		chunkPrefix = fmt.Sprintf("%s_chunk_", keyName)
	}

	var wg sync.WaitGroup
	successCount := 0
	var mutex sync.Mutex
	client := s.http

	for i, nodeURL := range ecNodes {
		wg.Add(1)
		go func(url string, chunkIndex int) {
			defer wg.Done()

			chunkKey := fmt.Sprintf("%s%d", chunkPrefix, chunkIndex)
			req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/delete/%s", url, chunkKey), nil)
			if err != nil {
				log.Printf("[%s] EC Delete failed (req creation error): %v\n", url, err)
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("[%s] EC Delete failed (network error): %v\n", url, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
				mutex.Lock()
				successCount++
				mutex.Unlock()
			} else {
				log.Printf("[%s] EC Delete failed (status: %d)\n", url, resp.StatusCode)
			}
		}(nodeURL, i)
	}

	wg.Wait()
	return successCount, nil
}

// DeleteFieldHybrid deletes both hot replicas and cold EC chunks.
func (s *Service) DeleteFieldHybrid(ctx context.Context, replicaNodes, ecNodes []string, metadata map[string]interface{}) (int, int, error) {
	log.Printf("%s[StorageOps] Deleting (Field Hybrid) key (metadata: %v)%s\n", config.Colors["RED"], metadata, config.Colors["RESET"])

	// 1. Extract info from metadata. Use fallbacks if metadata is empty (Blind Delete).
	keyName := "unknown_key"
	if k, ok := metadata["key_name"].(string); ok {
		keyName = k
	}

	var hotKey string
	if hk, ok := metadata["hot_key"].(string); ok {
		hotKey = hk
	} else {
		hotKey = fmt.Sprintf("%s_hot", keyName) // Guess for blind delete
	}

	var coldPrefix string
	if cp, ok := metadata["cold_prefix"].(string); ok {
		coldPrefix = cp
	} else {
		// Fix: Use 'cold_prefix' format for blind delete guess
		coldPrefix = fmt.Sprintf("%s_cold_chunk_", keyName)
	}

	// Fix: DeleteEC expects 'chunk_prefix', but Hybrid stores 'cold_prefix'.
	// We construct a specific metadata map for the EC deletion call.
	ecMetadata := map[string]interface{}{
		"chunk_prefix": coldPrefix,
		"key_name":     keyName,
	}

	var wg sync.WaitGroup
	var hotSuccess, coldSuccess int
	var hotErr, coldErr error

	// 2. Execute deletions concurrently
	wg.Add(2)

	// Task 1: Delete Hot Data
	go func() {
		defer wg.Done()
		hotSuccess, hotErr = s.DeleteReplication(ctx, replicaNodes, hotKey)
	}()

	// Task 2: Delete Cold Data
	go func() {
		defer wg.Done()
		coldSuccess, coldErr = s.DeleteEC(ctx, ecNodes, ecMetadata)
	}()

	wg.Wait()

	if hotErr != nil {
		return 0, 0, hotErr
	}
	if coldErr != nil {
		return 0, 0, coldErr
	}

	return hotSuccess, coldSuccess, nil
}