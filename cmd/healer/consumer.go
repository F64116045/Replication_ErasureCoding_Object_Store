package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"hybrid_distributed_store/internal/config"
)

// runWALConsumer listens to Redpanda for orphaned transactions (Crash Recovery)
func (h *HealerService) runWALConsumer(ctx context.Context, cfg Config) {
	log.Println("[WAL-Consumer] Listening for incoming transactions...")

	// Subscribe to WAL events
	err := h.mq.Consume(ctx, func(key, value []byte) error {
		// Deferred Verification: Wait for API Gateway to potentially finish writing
		
		// Copy data to avoid closure race conditions
		valCopy := make([]byte, len(value))
		copy(valCopy, value)
		keyCopy := string(key)

		// Non-blocking wait
		time.AfterFunc(cfg.RetryDelay, func() {
			// Use a new context as the original might be cancelled
			h.verifyAndRecover(context.Background(), keyCopy, valCopy)
		})

		return nil
	})

	if err != nil {
		log.Printf("[WAL-Consumer] Error starting consumer: %v", err)
	}
}

// verifyAndRecover checks if a transaction was lost and attempts to resurrect it
func (h *HealerService) verifyAndRecover(ctx context.Context, key string, walValue []byte) {
	var logEntry map[string]interface{}
	if err := json.Unmarshal(walValue, &logEntry); err != nil {
		log.Printf("[Recovery] Invalid WAL JSON for %s: %v", key, err)
		return
	}

	mainKey := logEntry["key_name"].(string)
	
	// 1. Check Etcd: Did the API Gateway succeed?
	metaKey := fmt.Sprintf("metadata/%s", mainKey)
	resp, err := h.etcd.Get(ctx, metaKey)
	if err != nil {
		log.Printf("[Recovery] Etcd error checking %s: %v", mainKey, err)
		return
	}

	// Case A: Metadata exists -> Transaction was successful. Ignore.
	if len(resp.Kvs) > 0 {
		return 
	}

	// Case B: Metadata missing -> Orphaned transaction found.
	log.Printf("%s[Recovery] ALERT: Potential lost transaction found: %s. Metadata missing.%s", config.Colors["YELLOW"], mainKey, config.Colors["RESET"])

	// 2. Attempt Resurrection
	h.resurrectData(ctx, mainKey, logEntry)
}

// resurrectData checks disk for files and reconstructs metadata if found
func (h *HealerService) resurrectData(ctx context.Context, key string, logEntry map[string]interface{}) {
	strategy := config.StorageStrategy(logEntry["strategy"].(string))
	details := logEntry["details"].(map[string]interface{})

	log.Printf("[Recovery] Checking storage nodes for orphaned data: %s", key)

	exists := false

	// Check if any data exists on storage nodes (Best Effort Check)
	switch strategy {
	case config.StrategyReplication:
		nodes, _ := h.getSortedNodes()
		count := 0
		for _, node := range nodes {
			if h.checkFileExists(node, key) {
				count++
			}
		}
		if count > 0 {
			exists = true
			log.Printf("[Recovery] Found %d replicas for %s.", count, key)
		}

	case config.StrategyEC:
		if prefix, ok := details["chunk_prefix"].(string); ok {
			if h.checkECAvailability(prefix) {
				exists = true
				log.Printf("[Recovery] Found sufficient EC chunks for %s.", key)
			}
		}
	
	case config.StrategyFieldHybrid:
		// For Hybrid, checking Hot Key existence is usually enough to resurrect
		if hotKey, ok := details["hot_key"].(string); ok {
			nodes, _ := h.getSortedNodes()
			for _, node := range nodes {
				if h.checkFileExists(node, hotKey) {
					exists = true
					break
				}
			}
		}
	}

	// If data exists but metadata is missing -> Resurrect Metadata
	if exists {
		log.Printf("%s[Recovery] Data exists on disk! Resurrecting Metadata for %s...%s", config.Colors["GREEN"], key, config.Colors["RESET"])
		
		// Reconstruct metadata from WAL entry
		meta := map[string]interface{}{
			"strategy":       string(strategy),
			"details":        details,
			"resurrected_by": "healer",
			// IMPORTANT: Mark as dirty so Polling Loop will fix replica counts later
			"is_dirty":       true, 
		}
		
		metaBytes, _ := json.Marshal(meta)
		if _, err := h.etcd.Put(ctx, "metadata/"+key, string(metaBytes)); err != nil {
			log.Printf("[Recovery] Failed to write metadata: %v", err)
		} else {
			log.Printf("[Recovery] Successfully resurrected %s", key)
		}
	} else {
		log.Printf("[Recovery] No data found on disk for %s. Transaction completely lost.", key)
	}
}

// checkECAvailability checks if enough chunks exist to be potentially recoverable
func (h *HealerService) checkECAvailability(chunkPrefix string) bool {
	_, allNodes := h.getSortedNodes()
	found := 0
	for i := 0; i < len(allNodes); i++ {
		chunkKey := fmt.Sprintf("%s%d", chunkPrefix, i)
		if h.checkFileExists(allNodes[i], chunkKey) {
			found++
		}
	}
	// We need at least K chunks to be useful
	return found >= config.K
}