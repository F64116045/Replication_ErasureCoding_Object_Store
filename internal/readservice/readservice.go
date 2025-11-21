package readservice

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
)

// Service implements IReadService.
// It orchestrates data retrieval across Replication, EC, and Hybrid strategies.
type Service struct {
	http  interfaces.IHttpClient
	ec    interfaces.IEcDriver
	utils interfaces.IUtilsSvc
}

// NewService creates a new ReadService with dependency injection.
func NewService(
	http interfaces.IHttpClient,
	ec interfaces.IEcDriver,
	utils interfaces.IUtilsSvc,
) interfaces.IReadService {
	return &Service{
		http:  http,
		ec:    ec,
		utils: utils,
	}
}

// CheckFirstWrite checks if a hot key already exists in the replica nodes.
// Returns true if it's the first write (key not found), false otherwise.
func (s *Service) CheckFirstWrite(ctx context.Context, replicaNodes []string, hotKey string) (bool, error) {
	resultChan := make(chan bool, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	
	// Query all replica nodes in parallel
	for _, nodeURL := range replicaNodes {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", url, hotKey), nil)
			if err != nil {
				return
			}
			resp, err := s.http.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			// If any node has the data, it's NOT a first write.
			if resp.StatusCode == http.StatusOK {
				select {
				case resultChan <- false:
					cancel()
				case <-ctx.Done():
				}
			}
		}(nodeURL)
	}
	wg.Wait()

	select {
	case result := <-resultChan:
		return result, nil
	default:
		// If no node returned 200 OK, treat as first write
		return true, nil
	}
}

// GetExistingColdFields retrieves and reconstructs cold data fields specifically for Hybrid updates.
func (s *Service) GetExistingColdFields(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("  %s[ReadService] Fetching existing cold fields concurrently...%s\n", config.Colors["CYAN"], config.Colors["RESET"])

	k, err := s.getIntFromMetadata(metadata, "k")
	if err != nil {
		k = config.K
	}
	originalLength, err := s.getIntFromMetadata(metadata, "original_length")
	if err != nil {
		return nil, fmt.Errorf("metadata missing 'original_length', cannot safely decode")
	}

	coldPrefix, _ := metadata["cold_prefix"].(string)
	if coldPrefix == "" {
		keyName, _ := metadata["key_name"].(string)
		coldPrefix = fmt.Sprintf("%s_cold_chunk_", keyName)
	}

	// Parallel Fetch Logic
	chunks := make([][]byte, len(ecNodes))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	healthyCount := 0

	for i, nodeURL := range ecNodes {
		wg.Add(1)
		go func(index int, url string) {
			defer wg.Done()
			chunkKey := fmt.Sprintf("%s%d", coldPrefix, index)
			
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", url, chunkKey), nil)
			if err != nil {
				return
			}

			resp, err := s.http.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				data, err := io.ReadAll(resp.Body)
				if err != nil {
					return
				}
				mutex.Lock()
				chunks[index] = data
				healthyCount++
				mutex.Unlock()
			}
		}(i, nodeURL)
	}
	wg.Wait()

	if healthyCount < k {
		log.Printf("  %s[ReadService] Insufficient chunks. Need %d, got %d%s\n", config.Colors["RED"], k, healthyCount, config.Colors["RESET"])
		return nil, fmt.Errorf("insufficient chunks (need %d, got %d)", k, healthyCount)
	}

	// EC Reconstruction
	if err := s.ec.Reconstruct(chunks); err != nil {
		log.Printf("  %s[ReadService] EC Reconstruction failed: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
		return nil, err
	}

	// Join shards
	var coldData bytes.Buffer
	for i := 0; i < k; i++ {
		if chunks[i] == nil {
			return nil, fmt.Errorf("chunk %d is nil after reconstruction", i)
		}
		coldData.Write(chunks[i])
	}

	// Truncate padding
	unpaddedData := coldData.Bytes()
	

	if len(unpaddedData) > originalLength {
		unpaddedData = unpaddedData[:originalLength]
	} else if len(unpaddedData) < originalLength {
		log.Printf("  %s[ReadService] Warning: Reconstructed data length (%d) < Original length (%d)%s\n", config.Colors["YELLOW"], len(unpaddedData), originalLength, config.Colors["RESET"])
	}

	coldFields, err := s.utils.Deserialize(unpaddedData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize cold fields: %v", err)
	}

	return coldFields, nil
}

// ReadReplication implements Strategy A: Simple Replication.
// It returns the data from the first responsive node.
func (s *Service) ReadReplication(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
	log.Printf("%s[ReadService] Strategy: Replication (key=%s)%s\n", config.Colors["GREEN"], key, config.Colors["RESET"])

	resultChan := make(chan []byte, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	for _, nodeURL := range replicaNodes {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", url, key), nil)
			if err != nil {
				return
			}
			resp, err := s.http.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				data, err := io.ReadAll(resp.Body)
				if err == nil {
					select {
					case resultChan <- data:
						cancel() // Stop other requests
					case <-ctx.Done():
					}
				}
			}
		}(nodeURL)
	}
	wg.Wait()

	select {
	case data := <-resultChan:
		return data, nil
	default:
		return nil, fmt.Errorf("key '%s' not found on any replica nodes", key)
	}
}

// ReadEC implements Strategy B: Erasure Coding.
// It fetches shards in parallel, reconstructs missing ones, and removes padding.
func (s *Service) ReadEC(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
	log.Printf("%s[ReadService] Strategy: EC (metadata=%v)%s\n", config.Colors["GREEN"], metadata, config.Colors["RESET"])

	k, err := s.getIntFromMetadata(metadata, "k")
	if err != nil {
		k = config.K
	}
	originalLength, err := s.getIntFromMetadata(metadata, "original_length")
	if err != nil {
		return nil, fmt.Errorf("metadata missing 'original_length'")
	}

	chunkPrefix, _ := metadata["cold_prefix"].(string)
	if chunkPrefix == "" {
		chunkPrefix, _ = metadata["chunk_prefix"].(string)
	}
	if chunkPrefix == "" {
		return nil, fmt.Errorf("metadata missing 'cold_prefix' or 'chunk_prefix'")
	}

	// Parallel Fetch
	chunks := make([][]byte, len(ecNodes))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	healthyCount := 0

	for i, nodeURL := range ecNodes {
		wg.Add(1)
		go func(index int, url string) {
			defer wg.Done()
			chunkKey := fmt.Sprintf("%s%d", chunkPrefix, index)
			
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", url, chunkKey), nil)
			if err != nil {
				return
			}
			resp, err := s.http.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				data, err := io.ReadAll(resp.Body)
				if err != nil {
					return
				}
				mutex.Lock()
				chunks[index] = data
				healthyCount++
				mutex.Unlock()
			}
		}(i, nodeURL)
	}
	wg.Wait()

	if healthyCount < k {
		return nil, fmt.Errorf("insufficient chunks (need %d, got %d)", k, healthyCount)
	}

	if err := s.ec.Reconstruct(chunks); err != nil {
		return nil, fmt.Errorf("EC reconstruction failed: %v", err)
	}

	var data bytes.Buffer
	for i := 0; i < k; i++ {
		if chunks[i] == nil {
			return nil, fmt.Errorf("chunk %d is nil post-reconstruction", i)
		}
		data.Write(chunks[i])
	}

	// Remove Padding
	unpaddedData := data.Bytes()
	if len(unpaddedData) < originalLength {
		return nil, fmt.Errorf("data corruption: reconstructed length (%d) < original (%d)", len(unpaddedData), originalLength)
	}
	
	// Truncate logic
	unpaddedData = unpaddedData[:originalLength]
	log.Printf("  %s[DEBUG] ReadEC: Truncated data length = %d%s\n", config.Colors["YELLOW"], len(unpaddedData), config.Colors["RESET"])

	return unpaddedData, nil
}

// ReadFieldHybrid implements Strategy C: Hybrid.
// It fetches Hot Data (Replication) and Cold Data (EC) in parallel.
func (s *Service) ReadFieldHybrid(ctx context.Context, replicaNodes, ecNodes []string, metadata map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("%s[ReadService] Strategy: Field Hybrid%s\n", config.Colors["GREEN"], config.Colors["RESET"])

	var wg sync.WaitGroup
	var hotFields map[string]interface{}
	var coldFields map[string]interface{}
	var hotErr, coldErr error

	hotKey, _ := metadata["hot_key"].(string)
	if hotKey == "" {
		return nil, fmt.Errorf("metadata missing 'hot_key'")
	}

	// 1. Fetch Hot Data (Replication)
	wg.Add(1)
	go func() {
		defer wg.Done()
		hotDataBytes, err := s.ReadReplication(ctx, replicaNodes, hotKey)
		if err != nil {
			hotErr = fmt.Errorf("failed to read hot data: %v", err)
			return
		}
		hotFields, hotErr = s.utils.Deserialize(hotDataBytes)
	}()

	// 2. Fetch Cold Data (EC)
	wg.Add(1)
	go func() {
		defer wg.Done()
		coldDataBytes, err := s.ReadEC(ctx, ecNodes, metadata)
		if err != nil {
			coldErr = fmt.Errorf("failed to read cold data: %v", err)
			return
		}
		coldFields, coldErr = s.utils.Deserialize(coldDataBytes)
	}()

	wg.Wait()

	if hotErr != nil {
		return nil, hotErr
	}
	if coldErr != nil {
		return nil, coldErr
	}

	return s.utils.MergeHotColdFields(hotFields, coldFields), nil
}

// helper: getIntFromMetadata safely extracts an integer from the generic map.
func (s *Service) getIntFromMetadata(metadata map[string]interface{}, key string) (int, error) {
	if metadata == nil {
		return 0, fmt.Errorf("metadata is nil")
	}
	value, ok := metadata[key]
	if !ok {
		return 0, fmt.Errorf("key '%s' not found in metadata", key)
	}
	switch v := value.(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("key '%s' is of unexpected type %T", key, value)
	}
}