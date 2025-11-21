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
	etcd "go.etcd.io/etcd/client/v3"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
)

// Service implements the write logic for Replication, EC, and Hybrid strategies.
// It handles distributed transactions using an Etcd-backed Write-Ahead Log .
type Service struct {
	etcd  interfaces.IEtcdClient
	http  interfaces.IHttpClient
	read  interfaces.IReadService
	ec    interfaces.IEcDriver
	utils interfaces.IUtilsSvc
}




// NewService creates a new WriteService.
func NewService(
	etcd interfaces.IEtcdClient,
	http interfaces.IHttpClient,
	read interfaces.IReadService,
	ec interfaces.IEcDriver,
	utils interfaces.IUtilsSvc,
) *Service {
	return &Service{
		etcd:  etcd,
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




// --- Write-Ahead Log Helpers ---

// createWALEntry creates a "PENDING" transaction record in Etcd.
// This ensures that if the process crashes during writing, the system knows there was an incomplete transaction.
func (s *Service) createWALEntry(
	ctx context.Context,
	key string,
	strategy config.StorageStrategy,
	metadataDetails map[string]interface{},
) (string, error) {

	txnID := fmt.Sprintf("txn/%s", uuid.New().String())

	logEntry := map[string]interface{}{
		"status":    "PENDING",
		"key_name":  key,
		"strategy":  string(strategy),
		"timestamp": time.Now().Unix(),
	}
	for k, v := range metadataDetails {
		logEntry[k] = v
	}

	b, err := json.Marshal(logEntry)
	if err != nil {
		return "", fmt.Errorf("failed to serialize WAL entry: %v", err)
	}

	_, err = s.etcd.Put(ctx, txnID, string(b))
	if err != nil {
		return "", fmt.Errorf("failed to write WAL to etcd: %v", err)
	}

	return txnID, nil
}




// finalizeWALEntry commits or rolls back the transaction.
// If success is true, it atomically updates the object metadata and deletes the WAL entry.
func (s *Service) finalizeWALEntry(
	ctx context.Context,
	txnID string,
	success bool,
	mainKey string,
	metadata map[string]interface{},
) error {

	if !success {
		// Mark as FAILED. The Healer service usually cleans this up.
		s.etcd.Put(ctx, txnID+"/status", "FAILED")
		return nil
	}

	metaKey := fmt.Sprintf("metadata/%s", mainKey)
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize final metadata: %v", err)
	}


	// Atomic Commit: Only update metadata if the WAL entry still exists and is valid.
	txn := s.etcd.Txn(ctx)
	resp, err := txn.If(
		etcd.Compare(etcd.Version(txnID), ">", 0),
	).Then(
		etcd.OpPut(metaKey, string(metaBytes)),
		etcd.OpDelete(txnID),
	).Commit()

	if err != nil {
		return fmt.Errorf("failed to commit WAL transaction: %v", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("WAL commit failed: txnID likely expired or modified")
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

	txnID, err := s.createWALEntry(ctx, key, config.StrategyReplication, walMeta)
	if err != nil {
		return nil, err
	}

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


	// Quorum check: stricter than 1, but here we require ALL nodes for simplicity
	if success < len(replicaNodes) {
		_ = s.finalizeWALEntry(ctx, txnID, false, key, nil)
		return nil, fmt.Errorf("replication failed: success %d / required %d", success, len(replicaNodes))
	}


	finalMeta := map[string]interface{}{
		"strategy":     string(config.StrategyReplication),
		"hot_version":  time.Now().UnixNano(),
		"cold_version": 0,
		"cold_hash":    "",
	}


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

	txnID, err := s.createWALEntry(ctx, key, config.StrategyEC, walMeta)
	if err != nil {
		return nil, err
	}

	// Split & Encode
	chunks, err := s.ec.Split(value)
	if err != nil {
		_ = s.finalizeWALEntry(ctx, txnID, false, key, nil)
		return nil, fmt.Errorf("EC split failed: %v", err)
	}
	if err := s.ec.Encode(chunks); err != nil {
		_ = s.finalizeWALEntry(ctx, txnID, false, key, nil)
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

	if success < totalChunks {
		_ = s.finalizeWALEntry(ctx, txnID, false, key, nil)
		return nil, fmt.Errorf("EC write failed: success %d / required %d", success, totalChunks)
	}

	finalMeta := map[string]interface{}{}
	for k, v := range walMeta {
		finalMeta[k] = v
	}
	finalMeta["hot_version"] = 0
	finalMeta["cold_version"] = time.Now().UnixNano()
	finalMeta["cold_hash"] = computeSHA256Hex(value)

	if err := s.finalizeWALEntry(ctx, txnID, true, key, finalMeta); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"chunks_written": success,
		"total_chunks":   totalChunks,
	}, nil
}



// --- Strategy C: Field-level Hybrid ---

// WriteFieldHybrid separates JSON into Hot/Cold fields.
// OPTIMIZATION: It calculates the hash of cold fields and skips EC operations
// if the cold data hasn't changed ("Pure Hot Update").
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

	// 2. Fetch Old Metadata (to check for changes)
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


	// Determine Optimization: Is this a Pure Hot Update?
	isPureHot := (newColdHash == oldColdHash)
	if hotOnly {
		log.Printf("[HYBRID] TraceID=%s Forced HotOnly mode.\n", traceID)
		isPureHot = true
	}


	// 4. Create WAL Entry
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


	// 5. Write Hot Fields (Replication) - Always executed
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



	// 6. Write Cold Fields (EC) - Executed ONLY if cold data changed
	var coldMu sync.Mutex
	coldSuccess := 0
	totalCold := 0

	if !isPureHot {
		log.Printf("[HYBRID] TraceID=%s Cold data changed. Performing EC Split/Encode.\n", traceID)
		
		chunks, err := s.ec.Split(newColdBytes)
		if err != nil {
			_ = s.finalizeWALEntry(ctx, txnID, false, key, nil)
			return nil, fmt.Errorf("EC split failed: %v", err)
		}

		if err := s.ec.Encode(chunks); err != nil {
			_ = s.finalizeWALEntry(ctx, txnID, false, key, nil)
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
		_ = s.finalizeWALEntry(ctx, txnID, false, key, nil)
		return nil, fmt.Errorf("Hybrid hot write failed: success %d / required %d", hotSuccess, len(replicaNodes))
	}
	if !isPureHot && coldSuccess < totalCold {
		_ = s.finalizeWALEntry(ctx, txnID, false, key, nil)
		return nil, fmt.Errorf("Hybrid cold chunks write failed: success %d / required %d", coldSuccess, totalCold)
	}


	// 7. Finalize Metadata
	finalMeta := map[string]interface{}{}
	for k, v := range walMeta {
		finalMeta[k] = v
	}

	finalMeta["hot_version"] = time.Now().UnixNano()
	// If cold data didn't change, keep the old version and hash
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
