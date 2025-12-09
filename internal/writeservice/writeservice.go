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
// and Etcd for Metadata storage. This separation ensures high throughput for
// write intents while maintaining strong consistency for metadata.
type Service struct {
	etcd  interfaces.IEtcdClient
	mq    *mq.Client // Redpanda Client for high-throughput WAL
	http  interfaces.IHttpClient
	read  interfaces.IReadService
	ec    interfaces.IEcDriver
	utils interfaces.IUtilsSvc
}

// NewService creates a new WriteService with necessary dependencies.
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

// computeSHA256Hex generates a SHA256 hash string for data fingerprinting.
func computeSHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// --- Write-Ahead Log Helpers (Redpanda Version) ---

// createWALEntry writes the "PENDING" transaction record to Redpanda (Kafka).
// Writing to Redpanda is significantly faster than Etcd due to sequential I/O,
// allowing for higher write throughput.
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

	// Write to Redpanda synchronously to ensure durability.
	// The object 'key' is used as the Kafka Partition Key to preserve ordering if needed.
	if err := s.mq.ProduceSync(ctx, key, valBytes); err != nil {
		return "", fmt.Errorf("failed to write WAL to Redpanda: %v", err)
	}

	return txnID, nil
}

// finalizeWALEntry commits the metadata to Etcd.
// Unlike traditional database WALs, we do not delete the log from Redpanda immediately
// after commit. Instead, we update the source of truth (Etcd Metadata) to mark the
// data as valid and visible.
func (s *Service) finalizeWALEntry(
	ctx context.Context,
	txnID string,
	success bool,
	mainKey string,
	metadata map[string]interface{},
) error {

	if !success {
		// If the operation failed, we simply do nothing here.
		// The Healer service is responsible for detecting stalled PENDING logs
		// in Redpanda and reconciling the state eventually.
		return nil
	}

	metaKey := fmt.Sprintf("metadata/%s", mainKey)
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize final metadata: %v", err)
	}

	// Commit to Etcd.
	// This is the point of "Strong Consistency". Once this succeeds,
	// the data is globally visible to readers.
	_, err = s.etcd.Put(ctx, metaKey, string(metaBytes))
	if err != nil {
		return fmt.Errorf("failed to commit metadata to Etcd: %v", err)
	}

	return nil
}

// --- Strategy A: Replication ---

// WriteReplication writes a full copy of the data to all replica nodes.
func (s *Service) WriteReplication(
	ctx context.Context,
	replicaNodes []string,
	key string,
	value []byte,
) (map[string]interface{}, error) {

	walMeta := map[string]interface{}{
		"strategy": config.StrategyReplication,
	}

	// Step 1: Write Intent to WAL (Redpanda)
	txnID, err := s.createWALEntry(ctx, key, config.StrategyReplication, walMeta)
	if err != nil {
		return nil, err
	}

	// Step 2: Write Payload to Storage Nodes
	// Since storage nodes implement async write-back I/O, this operation is non-blocking.
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0

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
				mu.Unlock()
			}
		}(url)
	}
	wg.Wait()

	// Quorum check
	if success < len(replicaNodes) {
		return nil, fmt.Errorf("replication failed: success %d / required %d", success, len(replicaNodes))
	}

	finalMeta := map[string]interface{}{
		"strategy":     string(config.StrategyReplication),
		"hot_version":  time.Now().UnixNano(),
		"cold_version": 0,
		"cold_hash":    "",
	}

	// Step 3: Commit Metadata to Etcd
	if err := s.finalizeWALEntry(ctx, txnID, true, key, finalMeta); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"nodes_written": replicaNodes,
	}, nil
}

// --- Strategy B: Erasure Coding (EC) ---

// WriteEC splits data into shards, encodes them, and distributes them to EC nodes.
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

	// 1. Write Intent to WAL
	txnID, err := s.createWALEntry(ctx, key, config.StrategyEC, walMeta)
	if err != nil {
		return nil, err
	}

	// 2. Split & Encode (CPU Intensive)
	chunks, err := s.ec.Split(value)
	if err != nil {
		return nil, fmt.Errorf("EC split failed: %v", err)
	}
	if err := s.ec.Encode(chunks); err != nil {
		return nil, fmt.Errorf("EC encode failed: %v", err)
	}

	// 3. Write Shards to Storage Nodes
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

	if success < totalChunks {
		return nil, fmt.Errorf("EC write failed: success %d / required %d", success, totalChunks)
	}

	finalMeta := map[string]interface{}{}
	for k, v := range walMeta {
		finalMeta[k] = v
	}
	finalMeta["hot_version"] = 0
	finalMeta["cold_version"] = time.Now().UnixNano()
	finalMeta["cold_hash"] = computeSHA256Hex(value)

	// 4. Commit Metadata to Etcd
	if err := s.finalizeWALEntry(ctx, txnID, true, key, finalMeta); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"chunks_written": success,
		"total_chunks":   totalChunks,
	}, nil
}

// --- Strategy C: Field-level Hybrid ---

// WriteFieldHybrid intelligently separates JSON into Hot and Cold fields.
// It computes the hash of the cold part to detect if it has changed.
// If the cold data is unchanged ("Pure Hot Update"), it skips the expensive EC operations.
func (s *Service) WriteFieldHybrid(
	ctx context.Context,
	replicaNodes, ecNodes []string,
	key string,
	dataDict map[string]interface{},
	hotOnly bool,
) (map[string]interface{}, error) {

	traceID := uuid.New().String()
	log.Printf("[HYBRID] Start TraceID=%s Key=%s HotOnly=%v\n", traceID, key, hotOnly)

	hotKey := fmt.Sprintf("%s_hot", key)
	coldPrefix := fmt.Sprintf("%s_cold_chunk_", key)

	// 1. Separate Fields
	newHot, newCold := s.utils.SeparateHotColdFields(dataDict)

	// 2. Fetch Old Metadata from Etcd (Read Operation)
	// We need this to compare the cold data hash.
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

	// 3. Compute New Cold Hash
	newColdBytes, _ := s.utils.Serialize(newCold)
	newColdHash := computeSHA256Hex(newColdBytes)

	// 4. Determine Optimization Path
	// If hashes match, we skip EC encoding and writing, saving significant I/O.
	isPureHot := (newColdHash == oldColdHash)
	if hotOnly {
		log.Printf("[HYBRID] TraceID=%s Forced HotOnly mode.\n", traceID)
		isPureHot = true
	}

	// 5. Write Intent to WAL
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

	// 6. Write Hot Fields (Replication) - This is always executed as hot data is assumed to change.
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

	// 7. Write Cold Fields (EC) - Executed ONLY if cold data actually changed.
	var coldMu sync.Mutex
	coldSuccess := 0
	totalCold := 0

	if !isPureHot {
		log.Printf("[HYBRID] TraceID=%s Cold data changed. Performing EC Split/Encode.\n", traceID)
		
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

	// Validate Success Rates
	if hotSuccess < len(replicaNodes) {
		return nil, fmt.Errorf("Hybrid hot write failed: success %d / required %d", hotSuccess, len(replicaNodes))
	}
	if !isPureHot && coldSuccess < totalCold {
		return nil, fmt.Errorf("Hybrid cold chunks write failed: success %d / required %d", coldSuccess, totalCold)
	}

	// 8. Commit Metadata to Etcd
	finalMeta := map[string]interface{}{}
	for k, v := range walMeta {
		finalMeta[k] = v
	}

	finalMeta["hot_version"] = time.Now().UnixNano()
	// Maintain old cold version/hash if pure hot update
	if isPureHot {
		finalMeta["cold_version"] = oldColdVersion
		finalMeta["cold_hash"] = oldColdHash
	} else {
		finalMeta["cold_version"] = time.Now().UnixNano()
		finalMeta["cold_hash"] = newColdHash
	}

	if err := s.finalizeWALEntry(ctx, txnID, true, key, finalMeta); err != nil {
		return nil, err
	}

	opType := "Cold Update"
	if isPureHot {
		opType = "Pure Hot Update"
	}
	log.Printf("[HYBRID] TraceID=%s Completed. Type=%s\n", traceID, opType)

	return map[string]interface{}{
		"trace_id":            traceID,
		"is_pure_hot_update":  isPureHot,
		"hot_nodes_written":   hotSuccess,
		"cold_chunks_written": coldSuccess,
		"operation_type":      opType,
	}, nil
}