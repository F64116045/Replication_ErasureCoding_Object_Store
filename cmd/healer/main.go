package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency" // [Added] Import for Leader Election

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	etcdclient "hybrid_distributed_store/internal/etcd"
	"hybrid_distributed_store/internal/httpclient"
	"hybrid_distributed_store/internal/interfaces"
	"hybrid_distributed_store/internal/mq"
	"hybrid_distributed_store/internal/utils"
)

// HealerService orchestrates the self-healing process.
type HealerService struct {
	etcd       *etcd.Client
	mq         *mq.Client
	httpClient *http.Client

	ec    interfaces.IEcDriver
	utils interfaces.IUtilsSvc

	// Service Discovery State
	activeNodeURLs map[string]string
	nodeLock       sync.RWMutex
}

// Config 存放 Loop 設定
type Config struct {
	CheckInterval time.Duration
	LeaderKey     string // [Added] Etcd key for election
}

func main() {
	log.Println("[Healer] Starting Production-Grade Recovery Service (Polling Mode)...")

	// 1. Initialize Dependencies
	etcdCl := etcdclient.GetClient()
	mqCl := mq.NewClient(true)
	defer mqCl.Close()

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
	go service.watchNodes(context.Background())

	// 3. Setup Loop Configuration
	cfg := Config{
		CheckInterval: 30 * time.Second,
		LeaderKey:     "/healer/leader", // [Added] Define the election key
	}

	// 4. Graceful Shutdown Context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived signal: %s. Shutting down...\n", sig)
		cancel()
	}()

	// 5. Start Control Loop with Leader Election
	if err := service.runWithLeaderElection(ctx, cfg); err != nil {
		log.Fatalf("Healer service failed: %v", err)
	}

	log.Println("Healer Service stopped successfully.")
}

// runWithLeaderElection handles the leadership campaign and runs the loop only when elected
func (h *HealerService) runWithLeaderElection(ctx context.Context, cfg Config) error {
	// 1. Create an Etcd Session (TTL handles heartbeat)
	// If this healer dies, the TTL expires and Etcd releases the lock.
	session, err := concurrency.NewSession(h.etcd, concurrency.WithTTL(15))
	if err != nil {
		return fmt.Errorf("failed to create etcd session: %w", err)
	}
	defer session.Close()

	// 2. Create an Election object
	election := concurrency.NewElection(session, cfg.LeaderKey)

	log.Println("[Election] Campaigning for leadership...")
	
	// 3. Campaign (Block until we become leader)
	// This will block here if another instance is already the leader.
	if err := election.Campaign(ctx, "healer-node-"+os.Getenv("HOSTNAME")); err != nil {
		if err == context.Canceled {
			return nil // Normal shutdown during campaign
		}
		return fmt.Errorf("campaign failed: %w", err)
	}

	log.Println("===================================================")
	log.Printf("%s[Election] WINNER! I am the Leader now.%s", config.Colors["GREEN"], config.Colors["RESET"])
	log.Println("===================================================")

	// 4. Run the Control Loop (Only reachable by Leader)
	// We run this in a blocking manner until ctx is cancelled or session expires
	errChan := make(chan error, 1)
	go func() {
		errChan <- h.runControlLoop(ctx, cfg)
	}()

	select {
	case <-ctx.Done():
		// User requested shutdown
		log.Println("[Election] Resigning leadership...")
		ctxResign, cancelResign := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelResign()
		election.Resign(ctxResign)
		return nil
	case <-session.Done():
		// Etcd session lost (network partition or etcd down)
		return fmt.Errorf("lost etcd session ownership")
	case err := <-errChan:
		return err
	}
}

// runControlLoop 執行週期性檢查
func (h *HealerService) runControlLoop(ctx context.Context, cfg Config) error {
	log.Printf("[Healer] Entering Control Loop (Interval: %v)...", cfg.CheckInterval)
	
	ticker := time.NewTicker(cfg.CheckInterval)
	defer ticker.Stop()

	// 啟動時先跑一次
	h.performHealthCheck(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			h.performHealthCheck(ctx)
		}
	}
}

// performHealthCheck 掃描所有 Metadata 並觸發修復
func (h *HealerService) performHealthCheck(ctx context.Context) {
	log.Println("[Loop] Starting full system health scan...")

	// 1. Scan all metadata keys
	// 注意：在生產環境如果 Key 非常多，這裡應該用 Paging (etcd.WithLimit)
	resp, err := h.etcd.Get(ctx, "metadata/", etcd.WithPrefix())
	if err != nil {
		log.Printf("[Loop] Failed to scan metadata: %v", err)
		return
	}

	log.Printf("[Loop] Found %d objects in metadata. Verifying health...", len(resp.Kvs))

	var wg sync.WaitGroup
	// 限制併發數，避免掃描時把 Storage Node 打掛
	sem := make(chan struct{}, 10) // Max 10 concurrent repairs

	for _, kv := range resp.Kvs {
		wg.Add(1)
		sem <- struct{}{} // Acquire token

		go func(key string, value []byte) {
			defer wg.Done()
			defer func() { <-sem }() // Release token

			// 解析 Metadata，這裡假設 Metadata 結構跟之前的 logEntry 類似
			// 包含 strategy 和 details
			if err := h.healObject(key, value); err != nil {
				log.Printf("[Loop] Error healing %s: %v", key, err)
			}
		}(string(kv.Key), kv.Value)
	}

	wg.Wait()
	log.Println("[Loop] Health scan completed.")
}

// healObject 是原本 processWalEntry 的改版，直接針對物件進行檢查與修復
func (h *HealerService) healObject(etcdKey string, etcdValue []byte) error {
	// key format: "metadata/filename" -> mainKey: "filename"
	mainKey := strings.TrimPrefix(etcdKey, "metadata/")

	var meta map[string]interface{}
	if err := json.Unmarshal(etcdValue, &meta); err != nil {
		return fmt.Errorf("invalid json in metadata: %v", err)
	}

	// 容錯檢查：確保欄位存在
	strategyRaw, ok := meta["strategy"]
	if !ok {
		// 可能是舊資料或格式不對，跳過
		return nil
	}

	// 安全轉型
	strategyStr, ok := strategyRaw.(string)
	if !ok {
		return fmt.Errorf("strategy field is not a string, got %T", strategyRaw)
	}
	strategy := config.StorageStrategy(strategyStr)

	// 取得 Details (但不強制一定要有，因為 Replication 可能不需要)
	var details map[string]interface{}
	if detailsRaw, ok := meta["details"]; ok {
		if d, ok := detailsRaw.(map[string]interface{}); ok {
			details = d
		}
	}

	// 根據策略進行檢查
	switch strategy {
	case config.StrategyReplication:
		// Replication 策略通常只需要 Key，不需要額外的 details
		h.auditAndRepairReplication(mainKey)

	case config.StrategyEC:
		if details == nil {
			return fmt.Errorf("missing details for EC strategy")
		}
		if prefix, ok := details["chunk_prefix"].(string); ok {
			h.auditAndRepairEC(prefix, mainKey)
		}

	case config.StrategyFieldHybrid:
		if details == nil {
			return fmt.Errorf("missing details for Hybrid strategy")
		}
		hotKey, _ := details["hot_key"].(string)
		chunkPrefix, _ := details["cold_prefix"].(string)

		if hotKey != "" {
			h.auditAndRepairReplication(hotKey)
		}
		if chunkPrefix != "" {
			h.auditAndRepairEC(chunkPrefix, mainKey)
		}
	}

	return nil
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
		// Log as debug only to reduce noise
		// log.Printf("\033[31m[Debug] Network Error accessing %s: %v\033[0m", url, err)
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

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