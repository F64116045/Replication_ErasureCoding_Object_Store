package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	// "os" // [Removed] Unused import
	"sort"
	"strings"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	etcdclient "hybrid_distributed_store/internal/etcd"
	"hybrid_distributed_store/internal/httpclient"
	"hybrid_distributed_store/internal/interfaces" // [Added] For interface types
	"hybrid_distributed_store/internal/mq"
	"hybrid_distributed_store/internal/utils"
)

// HealerService orchestrates the self-healing process.
type HealerService struct {
	etcd       *etcd.Client
	mq         *mq.Client
	httpClient *http.Client
	
	// [Fixed] Use Interface types instead of concrete structs
	// ec.NewService() likely returns interfaces.IEcDriver
	// utils.NewService() likely returns interfaces.IUtilsSvc
	ec    interfaces.IEcDriver
	utils interfaces.IUtilsSvc

	// Service Discovery State
	activeNodeURLs map[string]string
	nodeLock       sync.RWMutex
}

func main() {
	log.Println("[Healer] Starting Production-Grade Recovery Service...")

	// 1. Initialize Dependencies
	etcdCl := etcdclient.GetClient()
	mqCl := mq.NewClient(true) // Consumer Mode
	defer mqCl.Close()
	
	// Configure HTTP client with timeouts for reliability
	httpCl := httpclient.GetClient()
	httpCl.Timeout = 2 * time.Second

	service := &HealerService{
		etcd:           etcdCl,
		mq:             mqCl,
		httpClient:     httpCl,
		ec:             ec.NewService(),
		utils:          utils.NewService(),
		activeNodeURLs: make(map[string]string),
	}

	// 2. Start Service Discovery (Background)
	// Healer needs to know valid storage nodes to perform repairs
	go service.watchNodes(context.Background())

	// 3. Start Consuming WAL (Main Loop)
	ctx := context.Background()
	log.Println("[Healer] Watching WAL events for inconsistencies...")

	err := mqCl.Consume(ctx, service.processWalEntry)
	if err != nil {
		log.Fatalf("[Healer] Consumer crashed: %v", err)
	}
}

// --- Service Discovery ---

func (h *HealerService) watchNodes(ctx context.Context) {
	keyPrefix := "nodes/health/"
	
	// Initial fetch
	resp, err := h.etcd.Get(ctx, keyPrefix, etcd.WithPrefix())
	if err == nil {
		h.nodeLock.Lock()
		for _, kv := range resp.Kvs {
			h.updateNode(string(kv.Key), string(kv.Value))
		}
		h.nodeLock.Unlock()
	}

	// Watch loop
	watchChan := h.etcd.Watch(ctx, keyPrefix, etcd.WithPrefix())
	for watchResp := range watchChan {
		h.nodeLock.Lock()
		for _, event := range watchResp.Events {
			if event.Type == etcd.EventTypePut {
				h.updateNode(string(event.Kv.Key), string(event.Kv.Value))
			} else if event.Type == etcd.EventTypeDelete {
				parts := strings.Split(string(event.Kv.Key), "/")
				if len(parts) >= 3 {
					delete(h.activeNodeURLs, parts[2])
				}
			}
		}
		h.nodeLock.Unlock()
	}
}

func (h *HealerService) updateNode(key, val string) {
	parts := strings.Split(key, "/")
	if len(parts) >= 3 {
		nodeName := parts[2]
		if _, ok := config.ExpectedNodeNames[nodeName]; ok {
			h.activeNodeURLs[nodeName] = val
		}
	}
}

// getSortedNodes returns a deterministic list of nodes to locate data
func (h *HealerService) getSortedNodes() ([]string, []string) {
	h.nodeLock.RLock()
	defer h.nodeLock.RUnlock()

	allNodes := make([]string, 0, len(h.activeNodeURLs))
	for _, url := range h.activeNodeURLs {
		allNodes = append(allNodes, url)
	}
	sort.Strings(allNodes)

	// Same logic as API Gateway
	replicaNodes := allNodes
	if len(allNodes) > 3 {
		replicaNodes = allNodes[:3]
	}
	return replicaNodes, allNodes
}

// --- Core Logic: WAL Processing ---

func (h *HealerService) processWalEntry(key, value []byte) error {
	var logEntry map[string]interface{}
	if err := json.Unmarshal(value, &logEntry); err != nil {
		log.Printf("[Healer] Error parsing log: %v", err)
		return nil 
	}

	// [Fixed] Removed unused txnID variable to fix build error
	// txnID := logEntry["txn_id"].(string)
	
	mainKey := logEntry["key_name"].(string)
	strategy := config.StorageStrategy(logEntry["strategy"].(string))
	
	// 1. Consistency Check: Check Etcd State
	metaKey := fmt.Sprintf("metadata/%s", mainKey)
	resp, err := h.etcd.Get(context.Background(), metaKey)
	if err != nil {
		return err
	}

	if len(resp.Kvs) == 0 {
		// Metadata missing: Transaction failed or API crashed before commit.
		// In a rigorous system, we might perform "Rollback" (Delete garbage).
		// For now, we skip.
		log.Printf("[Healer] ALERT: Data Inconsistency Detected!")
        log.Printf("   Txn: (from log)") 
        log.Printf("   Key: %s", mainKey)
        log.Printf("   Status: Missing in Etcd (Write Lost)")
		return nil
	}

	// 2. Data Audit & Repair: Metadata exists, so data MUST exist.
	details := logEntry["details"].(map[string]interface{})

	switch strategy {
	case config.StrategyReplication:
		h.auditAndRepairReplication(mainKey)
	case config.StrategyEC:
		chunkPrefix := details["chunk_prefix"].(string)
		h.auditAndRepairEC(chunkPrefix, mainKey)
	case config.StrategyFieldHybrid:
		hotKey := details["hot_key"].(string)
		chunkPrefix := details["cold_prefix"].(string)
		
		// Repair Hot
		h.auditAndRepairReplication(hotKey)
		// Repair Cold (EC)
		h.auditAndRepairEC(chunkPrefix, mainKey)
	}

	return nil
}

// --- Repair Logic: Replication ---

func (h *HealerService) auditAndRepairReplication(key string) {
	replicaNodes, _ := h.getSortedNodes()
	if len(replicaNodes) == 0 {
		return
	}

	var healthyNode string
	var missingNodes []string
	var wg sync.WaitGroup
	var lock sync.Mutex

	// 1. Audit: Check all nodes in parallel
	for _, node := range replicaNodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			if h.checkFileExists(n, key) {
				lock.Lock()
				if healthyNode == "" {
					healthyNode = n
				}
				lock.Unlock()
			} else {
				lock.Lock()
				missingNodes = append(missingNodes, n)
				lock.Unlock()
			}
		}(node)
	}
	wg.Wait()

	// 2. Repair
	if len(missingNodes) > 0 {
		if healthyNode == "" {
			log.Printf("%s[Healer] CRITICAL: Data lost for key %s. No healthy replicas.%s", config.Colors["RED"], key, config.Colors["RESET"])
			return
		}
		
		log.Printf("[Healer] Repairing Replication for key: %s. Missing on: %v", key, missingNodes)
		
		// Fetch data from healthy node
		data, err := h.fetchData(healthyNode, key)
		if err != nil {
			log.Printf("[Healer] Failed to fetch source data: %v", err)
			return
		}

		// Write to missing nodes
		for _, target := range missingNodes {
			if err := h.writeData(target, key, data); err != nil {
				log.Printf("[Healer] Failed to repair node %s: %v", target, err)
			} else {
				log.Printf("%s[Healer] Successfully repaired replica on %s%s", config.Colors["GREEN"], target, config.Colors["RESET"])
			}
		}
	}
}

// --- Repair Logic: Erasure Coding ---

func (h *HealerService) auditAndRepairEC(chunkPrefix, mainKey string) {
	_, allNodes := h.getSortedNodes()
	// Need K+M nodes for EC
	if len(allNodes) < config.K+config.M {
		return
	}

	shards := make([][]byte, config.K+config.M)
	missingIndices := []int{}
	var wg sync.WaitGroup
	var lock sync.Mutex

	// 1. Audit: Check chunks
	for i := 0; i < config.K+config.M; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Use deterministic node selection
			if idx >= len(allNodes) {
				return
			}
			node := allNodes[idx]
			chunkKey := fmt.Sprintf("%s%d", chunkPrefix, idx)

			data, err := h.fetchData(node, chunkKey)
			lock.Lock()
			defer lock.Unlock()
			
			if err == nil && len(data) > 0 {
				shards[idx] = data
			} else {
				missingIndices = append(missingIndices, idx)
			}
		}(i)
	}
	wg.Wait()

	// 2. Evaluate Health
	if len(missingIndices) == 0 {
		return // All healthy
	}

	availableCount := 0
	for _, s := range shards {
		if s != nil {
			availableCount++
		}
	}

	if availableCount < config.K {
		log.Printf("%s[Healer] CRITICAL: Not enough shards to reconstruct %s (%d/%d). Data Lost.%s", config.Colors["RED"], mainKey, availableCount, config.K, config.Colors["RESET"])
		return
	}

	// 3. Reconstruct
	log.Printf("[Healer] Reconstructing EC chunks for %s. Missing: %v", mainKey, missingIndices)
	
	if err := h.ec.Reconstruct(shards); err != nil {
		log.Printf("[Healer] Reconstruction failed: %v", err)
		return
	}

	// 4. Write back missing shards
	// Note: We need to re-verify which shards were reconstructed. Reconstruct modifies the 'shards' slice in place.
	for _, idx := range missingIndices {
		if idx >= len(allNodes) {
			continue
		}
		targetNode := allNodes[idx]
		chunkKey := fmt.Sprintf("%s%d", chunkPrefix, idx)
		
		if err := h.writeData(targetNode, chunkKey, shards[idx]); err != nil {
			log.Printf("[Healer] Failed to write reconstructed chunk to %s: %v", targetNode, err)
		} else {
			log.Printf("%s[Healer] Successfully repaired chunk %d on %s%s", config.Colors["GREEN"], idx, targetNode, config.Colors["RESET"])
		}
	}
}

// --- Helpers ---

func (h *HealerService) checkFileExists(nodeURL, key string) bool {
	// Use HTTP HEAD to check existence efficiently
	url := fmt.Sprintf("%s/retrieve/%s", nodeURL, key)
	resp, err := h.httpClient.Head(url)
	if err != nil {
		log.Printf("\033[31m[Debug] Network Error accessing %s: %v\033[0m", url, err)
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

    if resp.StatusCode != http.StatusOK {
        log.Printf("\033[31m[Debug] Node %s returned Error Status: %d\033[0m", url, resp.StatusCode)
        return false
    }
	return resp.StatusCode == http.StatusOK
}

func (h *HealerService) fetchData(nodeURL, key string) ([]byte, error) {
	resp, err := h.httpClient.Get(fmt.Sprintf("%s/retrieve/%s", nodeURL, key))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (h *HealerService) writeData(nodeURL, key string, data []byte) error {
	resp, err := h.httpClient.Post(
		fmt.Sprintf("%s/store?key=%s", nodeURL, key),
		"application/octet-stream",
		bytes.NewReader(data),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}