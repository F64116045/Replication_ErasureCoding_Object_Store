package writeservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
	"hybrid_distributed_store/internal/mq"
)

// Service implements the write logic using Redpanda for WAL (Write-Ahead Log)
// and Etcd for Metadata storage.
type Service struct {
	etcd  interfaces.IEtcdClient
	mq    *mq.Client
	http  interfaces.IHttpClient
	read  interfaces.IReadService
	ec    interfaces.IEcDriver
	utils interfaces.IUtilsSvc
}

// NewService creates a new WriteService.
func NewService(
	etcd interfaces.IEtcdClient,
	mqClient *mq.Client,
	http interfaces.IHttpClient,
	read interfaces.IReadService,
	ec interfaces.IEcDriver,
	utils interfaces.IUtilsSvc,
) *Service {
	return &Service{
		etcd:  etcd,
		mq:    mqClient,
		http:  http,
		read:  read,
		ec:    ec,
		utils: utils,
	}
}

func computeSHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// --- Write-Ahead Log Helpers ---

func (s *Service) createWALEntry(
	ctx context.Context,
	key string,
	strategy config.StorageStrategy,
	metadataDetails map[string]interface{},
) (string, error) {

	txnID := fmt.Sprintf("txn:%s", uuid.New().String())

	logEntry := map[string]interface{}{
		"txn_id":    txnID,
		"status":    "PENDING",
		"key_name":  key,
		"strategy":  string(strategy),
		"timestamp": time.Now().Unix(),
		"details":   metadataDetails,
	}

	valBytes, err := json.Marshal(logEntry)
	if err != nil {
		return "", fmt.Errorf("failed to serialize WAL entry: %v", err)
	}

	if err := s.mq.ProduceSync(ctx, key, valBytes); err != nil {
		return "", fmt.Errorf("failed to write WAL to Redpanda: %v", err)
	}

	return txnID, nil
}

func (s *Service) finalizeWALEntry(
	ctx context.Context,
	txnID string,
	success bool,
	mainKey string,
	metadata map[string]interface{},
) error {

	if !success {
		return nil
	}

	metaKey := fmt.Sprintf("metadata/%s", mainKey)
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize final metadata: %v", err)
	}

	_, err = s.etcd.Put(ctx, metaKey, string(metaBytes))
	if err != nil {
		return fmt.Errorf("failed to commit metadata to Etcd: %v", err)
	}

	return nil
}

// --- Strategy A: Replication ---

func (s *Service) WriteReplication(
	ctx context.Context,
	replicaNodes []string,
	key string,
	value []byte,
) (map[string]interface{}, error) {

	walMeta := map[string]interface{}{
		"strategy": config.StrategyReplication,
	}

	txnID, err := s.createWALEntry(ctx, key, config.StrategyReplication, walMeta)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	// 用來記錄哪些節點寫入成功，方便 Healer 除錯或 API 回傳
	writtenNodes := []string{}

	for _, url := range replicaNodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(ctx, "POST",
				fmt.Sprintf("%s/store?key=%s", n, key),
				bytes.NewReader(value),
			)
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := s.http.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				mu.Lock()
				success++
				writtenNodes = append(writtenNodes, n)
				mu.Unlock()
			} else {
				log.Printf("[WriteReplication] Failed to write to %s: %v", n, err)
			}
		}(url)
	}
	wg.Wait()

	// [CHANGE] Best Effort: 只要有 1 個寫入成功，我們就 Commit。
	// 剩下的交給 Healer 去修復。
	if success == 0 {
		return nil, fmt.Errorf("replication failed entirely: 0/%d nodes responded", len(replicaNodes))
	}

	finalMeta := map[string]interface{}{
		"strategy":     string(config.StrategyReplication),
		"hot_version":  time.Now().UnixNano(),
		"cold_version": 0,
		"cold_hash":    "",
	}

	// [ADDED] Explicitly mark as dirty if partial failure occurred
	isDirty := success < len(replicaNodes)
	if isDirty {
		finalMeta["is_dirty"] = true
		log.Printf("[PartialWrite] Key=%s marked dirty. %d/%d replicas written.", key, success, len(replicaNodes))
	}

	if err := s.finalizeWALEntry(ctx, txnID, true, key, finalMeta); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"nodes_written": writtenNodes,
		"status":        "committed",
		"partial":       isDirty,
	}, nil
}

// --- Strategy B: Erasure Coding (EC) ---

func (s *Service) WriteEC(
	ctx context.Context,
	ecNodes []string,
	key string,
	value []byte,
) (map[string]interface{}, error) {

	chunkPrefix := fmt.Sprintf("%s_cold_chunk_", key)

	walMeta := map[string]interface{}{
		"strategy":        config.StrategyEC,
		"k":               config.K,
		"m":               config.M,
		"chunk_prefix":    chunkPrefix,
		"original_length": len(value),
		"key_name":        key,
	}

	txnID, err := s.createWALEntry(ctx, key, config.StrategyEC, walMeta)
	if err != nil {
		return nil, err
	}

	chunks, err := s.ec.Split(value)
	if err != nil {
		return nil, fmt.Errorf("EC split failed: %v", err)
	}
	if err := s.ec.Encode(chunks); err != nil {
		return nil, fmt.Errorf("EC encode failed: %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	totalChunks := len(chunks)

	for i := range chunks {
		if i >= len(ecNodes) {
			break
		}
		wg.Add(1)
		go func(idx int, c []byte) {
			defer wg.Done()
			url := ecNodes[idx]
			req, _ := http.NewRequestWithContext(ctx, "POST",
				fmt.Sprintf("%s/store?key=%s%d", url, chunkPrefix, idx),
				bytes.NewReader(c),
			)
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := s.http.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}(i, chunks[i])
	}
	wg.Wait()

	// [CHANGE] EC Constraint: 必須至少寫入 K 份，資料才是可讀的。
	// 如果少於 K，就算 Commit 了也是壞檔，所以這裡必須報錯。
	if success < config.K {
		return nil, fmt.Errorf("EC write critical failure: %d/%d (Need at least %d to recover)", success, totalChunks, config.K)
	}

	finalMeta := map[string]interface{}{}
	for k, v := range walMeta {
		finalMeta[k] = v
	}
	finalMeta["hot_version"] = 0
	finalMeta["cold_version"] = time.Now().UnixNano()
	finalMeta["cold_hash"] = computeSHA256Hex(value)

	// [ADDED] Mark as dirty if any chunk failed
	isDirty := success < totalChunks
	if isDirty {
		finalMeta["is_dirty"] = true
		log.Printf("[PartialWrite] Key=%s marked dirty. %d/%d chunks written.", key, success, totalChunks)
	}

	if err := s.finalizeWALEntry(ctx, txnID, true, key, finalMeta); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"chunks_written": success,
		"total_chunks":   totalChunks,
		"partial":        isDirty,
	}, nil
}

// --- Strategy C: Field-level Hybrid ---

func (s *Service) WriteFieldHybrid(
	ctx context.Context,
	replicaNodes, ecNodes []string,
	key string,
	dataDict map[string]interface{},
	hotOnly bool,
) (map[string]interface{}, error) {

	traceID := uuid.New().String()

	hotKey := fmt.Sprintf("%s_hot", key)
	coldPrefix := fmt.Sprintf("%s_cold_chunk_", key)

	newHot, newCold := s.utils.SeparateHotColdFields(dataDict)

	metaKey := fmt.Sprintf("metadata/%s", key)
	resp, _ := s.etcd.Get(ctx, metaKey)

	oldColdHash := ""
	var oldColdVersion int64 = 0

	if len(resp.Kvs) > 0 {
		var oldMeta map[string]interface{}
		if err := json.Unmarshal(resp.Kvs[0].Value, &oldMeta); err == nil {
			if v, ok := oldMeta["cold_hash"].(string); ok {
				oldColdHash = v
			}
			switch v := oldMeta["cold_version"].(type) {
			case float64:
				oldColdVersion = int64(v)
			case int64:
				oldColdVersion = v
			}
		}
	}

	newColdBytes, _ := s.utils.Serialize(newCold)
	newColdHash := computeSHA256Hex(newColdBytes)

	isPureHot := (newColdHash == oldColdHash)
	if hotOnly {
		isPureHot = true
	}

	log.Printf("[HYBRID] Start TraceID=%s Key=%s PureHot=%v\n", traceID, key, isPureHot)

	walMeta := map[string]interface{}{
		"strategy":        config.StrategyFieldHybrid,
		"hot_key":         hotKey,
		"cold_prefix":     coldPrefix,
		"k":               config.K,
		"m":               config.M,
		"original_length": len(newColdBytes),
		"key_name":        key,
		"trace_id":        traceID,
	}

	txnID, err := s.createWALEntry(ctx, key, config.StrategyFieldHybrid, walMeta)
	if err != nil {
		return nil, err
	}

	// 1. Write Hot Fields (Replication)
	hotBytes, _ := s.utils.Serialize(newHot)
	var wg sync.WaitGroup
	var hotMu sync.Mutex
	hotSuccess := 0

	for _, url := range replicaNodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(ctx, "POST",
				fmt.Sprintf("%s/store?key=%s", n, hotKey),
				bytes.NewReader(hotBytes),
			)
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := s.http.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				hotMu.Lock()
				hotSuccess++
				hotMu.Unlock()
			}
		}(url)
	}

	// 2. Write Cold Fields (EC)
	var coldMu sync.Mutex
	coldSuccess := 0
	totalCold := 0

	if !isPureHot {
		chunks, err := s.ec.Split(newColdBytes)
		if err != nil {
			return nil, fmt.Errorf("EC split failed: %v", err)
		}
		if err := s.ec.Encode(chunks); err != nil {
			return nil, fmt.Errorf("EC encode failed: %v", err)
		}

		totalCold = len(chunks)

		for i := range chunks {
			wg.Add(1)
			go func(idx int, c []byte) {
				defer wg.Done()
				url := ecNodes[idx]
				req, _ := http.NewRequestWithContext(ctx, "POST",
					fmt.Sprintf("%s/store?key=%s%d", url, coldPrefix, idx),
					bytes.NewReader(c),
				)
				req.Header.Set("Content-Type", "application/octet-stream")

				resp, err := s.http.Do(req)
				if err == nil && resp.StatusCode == http.StatusOK {
					coldMu.Lock()
					coldSuccess++
					coldMu.Unlock()
				}
			}(i, chunks[i])
		}
	}

	wg.Wait()

	// [CHANGE] Hybrid Validation
	// Hot: 至少 1 份
	if hotSuccess == 0 {
		return nil, fmt.Errorf("Hybrid hot write failed entirely")
	}
	// Cold: 如果有變動，至少 K 份
	if !isPureHot && coldSuccess < config.K {
		return nil, fmt.Errorf("Hybrid cold write critical failure: %d/%d", coldSuccess, totalCold)
	}

	finalMeta := map[string]interface{}{}
	for k, v := range walMeta {
		finalMeta[k] = v
	}

	finalMeta["hot_version"] = time.Now().UnixNano()
	if isPureHot {
		finalMeta["cold_version"] = oldColdVersion
		finalMeta["cold_hash"] = oldColdHash
	} else {
		finalMeta["cold_version"] = time.Now().UnixNano()
		finalMeta["cold_hash"] = newColdHash
	}

	// [ADDED] Calculate dirty status for Hybrid
	isDirty := false
	if hotSuccess < len(replicaNodes) {
		isDirty = true
	}
	if !isPureHot && coldSuccess < totalCold {
		isDirty = true
	}
	if isDirty {
		finalMeta["is_dirty"] = true
		log.Printf("[PartialWrite] Hybrid Key=%s marked dirty. Hot: %d/%d, Cold: %d/%d", key, hotSuccess, len(replicaNodes), coldSuccess, totalCold)
	}

	if err := s.finalizeWALEntry(ctx, txnID, true, key, finalMeta); err != nil {
		return nil, err
	}

	opType := "Cold Update"
	if isPureHot {
		opType = "Pure Hot Update"
	}

	return map[string]interface{}{
		"trace_id":            traceID,
		"is_pure_hot_update":  isPureHot,
		"hot_nodes_written":   hotSuccess,
		"cold_chunks_written": coldSuccess,
		"operation_type":      opType,
		"partial":             isDirty,
	}, nil
}