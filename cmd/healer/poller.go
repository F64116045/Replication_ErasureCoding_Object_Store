package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"hybrid_distributed_store/internal/config"
)

// runControlLoop executes the periodic health check
func (h *HealerService) runControlLoop(ctx context.Context, cfg Config) {
	log.Printf("[Healer] Entering Control Loop (Interval: %v)...", cfg.CheckInterval)
	ticker := time.NewTicker(cfg.CheckInterval)
	defer ticker.Stop()

	// Run immediately on start
	h.performHealthCheck(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.performHealthCheck(ctx)
		}
	}
}

// performHealthCheck scans all metadata and triggers repairs
func (h *HealerService) performHealthCheck(ctx context.Context) {
	log.Println("[Loop] Starting full system health scan...")

	// 1. Scan all metadata keys
	resp, err := h.etcd.Get(ctx, "metadata/", etcd.WithPrefix())
	if err != nil {
		log.Printf("[Loop] Failed to scan metadata: %v", err)
		return
	}

	log.Printf("[Loop] Found %d objects in metadata. Verifying health...", len(resp.Kvs))

	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)

	for _, kv := range resp.Kvs {
		wg.Add(1)
		sem <- struct{}{}

		go func(key string, value []byte) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := h.healObject(ctx, key, value); err != nil {
				log.Printf("[Loop] Error healing %s: %v", key, err)
			}
		}(string(kv.Key), kv.Value)
	}

	wg.Wait()
	log.Println("[Loop] Health scan completed.")
}

// healObject verifies and repairs a single object
// [Modified] Check is_dirty flag and clear it if repair succeeds
func (h *HealerService) healObject(ctx context.Context, etcdKey string, etcdValue []byte) error {
	mainKey := strings.TrimPrefix(etcdKey, "metadata/")

	var meta map[string]interface{}
	if err := json.Unmarshal(etcdValue, &meta); err != nil {
		return fmt.Errorf("invalid json in metadata: %v", err)
	}

	// 1. Check Dirty Flag
	isDirty := false
	if v, ok := meta["is_dirty"].(bool); ok {
		isDirty = v
	}

	strategyRaw, ok := meta["strategy"]
	if !ok {
		return nil
	}
	strategyStr, ok := strategyRaw.(string)
	if !ok {
		return fmt.Errorf("strategy field is not a string")
	}
	strategy := config.StorageStrategy(strategyStr)

	var details map[string]interface{}
	if detailsRaw, ok := meta["details"]; ok {
		if d, ok := detailsRaw.(map[string]interface{}); ok {
			details = d
		}
	}

	// 2. Perform Audit & Repair
	// We modify audit functions to return error. nil means "Healthy" or "Repaired Successfully".
	var healthErr error

	switch strategy {
	case config.StrategyReplication:
		healthErr = h.auditAndRepairReplication(mainKey)

	case config.StrategyEC:
		if details == nil {
			healthErr = fmt.Errorf("missing details for EC strategy")
		} else if prefix, ok := details["chunk_prefix"].(string); ok {
			healthErr = h.auditAndRepairEC(prefix, mainKey)
		}

	case config.StrategyFieldHybrid:
		if details == nil {
			healthErr = fmt.Errorf("missing details for Hybrid strategy")
		} else {
			hotKey, _ := details["hot_key"].(string)
			chunkPrefix, _ := details["cold_prefix"].(string)

			// Hybrid is healthy only if BOTH parts are healthy
			if hotKey != "" {
				if err := h.auditAndRepairReplication(hotKey); err != nil {
					healthErr = err
				}
			}
			if healthErr == nil && chunkPrefix != "" {
				if err := h.auditAndRepairEC(chunkPrefix, mainKey); err != nil {
					healthErr = err
				}
			}
		}
	}

	// 3. Clear Dirty Flag if Healthy
	// 如果原本是 dirty，且現在 healthErr 為 nil (代表修好了或原本就沒壞)，我們就更新 Etcd
	if isDirty && healthErr == nil {
		log.Printf("[Healer] Cleaning dirty flag for %s...", mainKey)
		delete(meta, "is_dirty") // Remove the flag
		
		// Update Etcd
		newMetaBytes, _ := json.Marshal(meta)
		if _, err := h.etcd.Put(ctx, etcdKey, string(newMetaBytes)); err != nil {
			log.Printf("[Healer] Failed to clear dirty flag for %s: %v", mainKey, err)
		} else {
			log.Printf("%s[Healer] Dirty flag cleared for %s. System consistent.%s", config.Colors["GREEN"], mainKey, config.Colors["RESET"])
		}
	}

	return healthErr
}

// --- Repair Logic: Replication ---

// [Modified] Returns error if repair fails or data is lost
func (h *HealerService) auditAndRepairReplication(key string) error {
	replicaNodes, _ := h.getSortedNodes()
	if len(replicaNodes) == 0 {
		return fmt.Errorf("no available nodes")
	}

	var healthyNode string
	var missingNodes []string
	var wg sync.WaitGroup
	var lock sync.Mutex

	// 1. Audit
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
			return fmt.Errorf("critical data loss")
		}

		log.Printf("[Healer] Repairing Replication for key: %s. Missing on: %v", key, missingNodes)

		data, err := h.fetchData(healthyNode, key)
		if err != nil {
			log.Printf("[Healer] Failed to fetch source data: %v", err)
			return err
		}

		// Write to missing nodes
		failedRepairs := 0
		for _, target := range missingNodes {
			if err := h.writeData(target, key, data); err != nil {
				log.Printf("[Healer] Failed to repair node %s: %v", target, err)
				failedRepairs++
			} else {
				log.Printf("%s[Healer] Successfully repaired replica on %s%s", config.Colors["GREEN"], target, config.Colors["RESET"])
			}
		}

		if failedRepairs > 0 {
			return fmt.Errorf("failed to repair %d replicas", failedRepairs)
		}
	}

	return nil // All healthy or successfully repaired
}

// --- Repair Logic: Erasure Coding ---

// [Modified] Returns error if repair fails
func (h *HealerService) auditAndRepairEC(chunkPrefix, mainKey string) error {
	_, allNodes := h.getSortedNodes()
	if len(allNodes) < config.K+config.M {
		return fmt.Errorf("not enough nodes for EC")
	}

	shards := make([][]byte, config.K+config.M)
	missingIndices := []int{}
	var wg sync.WaitGroup
	var lock sync.Mutex

	// 1. Audit
	for i := 0; i < config.K+config.M; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
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

	// 2. Evaluate
	if len(missingIndices) == 0 {
		return nil // Healthy
	}

	availableCount := 0
	for _, s := range shards {
		if s != nil {
			availableCount++
		}
	}

	if availableCount < config.K {
		log.Printf("%s[Healer] CRITICAL: Not enough shards to reconstruct %s (%d/%d). Data Lost.%s", config.Colors["RED"], mainKey, availableCount, config.K, config.Colors["RESET"])
		return fmt.Errorf("critical data loss: not enough shards")
	}

	// 3. Reconstruct
	log.Printf("[Healer] Reconstructing EC chunks for %s. Missing: %v", mainKey, missingIndices)

	if err := h.ec.Reconstruct(shards); err != nil {
		log.Printf("[Healer] Reconstruction failed: %v", err)
		return err
	}

	// 4. Write Back
	failedRepairs := 0
	for _, idx := range missingIndices {
		if idx >= len(allNodes) {
			continue
		}
		targetNode := allNodes[idx]
		chunkKey := fmt.Sprintf("%s%d", chunkPrefix, idx)

		if err := h.writeData(targetNode, chunkKey, shards[idx]); err != nil {
			log.Printf("[Healer] Failed to write reconstructed chunk to %s: %v", targetNode, err)
			failedRepairs++
		} else {
			log.Printf("%s[Healer] Successfully repaired chunk %d on %s%s", config.Colors["GREEN"], idx, targetNode, config.Colors["RESET"])
		}
	}

	if failedRepairs > 0 {
		return fmt.Errorf("failed to repair %d chunks", failedRepairs)
	}

	return nil
}