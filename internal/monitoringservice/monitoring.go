package monitoringservice

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"hybrid_distributed_store/internal/httpclient"
)

// StorageInfoResponse matches the JSON structure returned by the storage_node /info endpoint.
type StorageInfoResponse struct {
	TotalSize int `json:"total_size"`
	// Add "total_keys" or other fields if needed
}

// FetchNodeStatus checks the /health endpoint of all listed nodes concurrently.
func FetchNodeStatus(ctx context.Context, nodeList []string) (map[string]string, error) {
	log.Println("[Monitoring] Checking node status...")

	client := httpclient.GetClient()
	statusMap := make(map[string]string)
	var wg sync.WaitGroup
	var mutex sync.Mutex // Mutex for safe concurrent map writes

	for _, nodeURL := range nodeList {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			// Create context with timeout
			reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(reqCtx, "GET", url+"/health", nil)
			if err != nil {
				log.Printf("Failed to create /health request for %s: %v\n", url, err)
				return
			}

			status := "unhealthy" // Default status
			resp, err := client.Do(req)
			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					status = "healthy"
				}
			}

			mutex.Lock()
			statusMap[url] = status
			mutex.Unlock()

		}(nodeURL)
	}

	wg.Wait()
	return statusMap, nil
}

// FetchStorageUsage aggregates disk usage statistics from all nodes concurrently.
func FetchStorageUsage(ctx context.Context, nodeList []string) (map[string]interface{}, error) {
	log.Println("[Monitoring] Checking storage usage...")

	client := httpclient.GetClient()
	totalSize := int64(0) // Use int64 to prevent overflow
	activeNodes := 0

	var wg sync.WaitGroup
	var mutex sync.Mutex // Mutex for safe concurrent updates

	for _, nodeURL := range nodeList {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(reqCtx, "GET", url+"/info", nil)
			if err != nil {
				log.Printf("Failed to create /info request for %s: %v\n", url, err)
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Failed to execute /info request for %s: %v\n", url, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					return
				}

				var info StorageInfoResponse
				if err := json.Unmarshal(body, &info); err == nil {
					mutex.Lock()
					totalSize += int64(info.TotalSize)
					if info.TotalSize > 0 {
						activeNodes++
					}
					mutex.Unlock()
				}
			}
		}(nodeURL)
	}

	wg.Wait()

	return map[string]interface{}{
		"total_system_size":      totalSize,
		"active_nodes_with_data": activeNodes,
		"total_nodes_queried":    len(nodeList),
	}, nil
}