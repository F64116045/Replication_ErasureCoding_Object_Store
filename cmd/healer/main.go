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
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	etcdclient "hybrid_distributed_store/internal/etcd"
	"hybrid_distributed_store/internal/httpclient"
	"hybrid_distributed_store/internal/interfaces"
	"hybrid_distributed_store/internal/storageops"
)


// --- Globals for Service Discovery ---
var (
	ActiveNodeURLs  = make(map[string]string)
	NodeListLock    = &sync.RWMutex{}
	NodesReadyEvent = make(chan struct{})
	nodesReady      = false
	MyID            = os.Getenv("HOSTNAME") // Unique ID for Leader Election
)


// watchNodesTask maintains the list of healthy storage nodes.
func watchNodesTask(ctx context.Context, etcdClient *etcd.Client) {
	keyPrefix := "nodes/health/"
	log.Printf("%s[HEALER-Watcher] Service Discovery started. Watching: '%s'%s\n", config.Colors["CYAN"], keyPrefix, config.Colors["RESET"])

	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s[HEALER-Watcher] PANIC: %v%s\n", config.Colors["RED"], r, config.Colors["RESET"])
			log.Println(string(debug.Stack()))
		}
	}()

	// 1. Initial Fetch
	rangeResp, err := etcdClient.Get(ctx, keyPrefix, etcd.WithPrefix())
	if err != nil {
		log.Printf("%s[HEALER-Watcher] Initial fetch failed: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
	} else {
		NodeListLock.Lock()
		ActiveNodeURLs = make(map[string]string)
		for _, kv := range rangeResp.Kvs {
			parts := strings.Split(string(kv.Key), "/")
			if len(parts) < 3 { continue }
			nodeName := parts[2]
			nodeURL := string(kv.Value)
			if _, ok := config.ExpectedNodeNames[nodeName]; ok {
				ActiveNodeURLs[nodeName] = nodeURL
			}
		}
		log.Printf("%s[HEALER-Watcher] Initial Nodes: %v%s\n", config.Colors["CYAN"], getKeys(ActiveNodeURLs), config.Colors["RESET"])
		if len(ActiveNodeURLs) >= config.K && !nodesReady {
			nodesReady = true
			close(NodesReadyEvent)
		}
		NodeListLock.Unlock()
	}


	// 2. Watch Loop
	watchChan := etcdClient.Watch(ctx, keyPrefix, etcd.WithPrefix())
	for watchResp := range watchChan {
		if err := watchResp.Err(); err != nil {
			log.Printf("%s[HEALER-Watcher] Watch error: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
			time.Sleep(2 * time.Second)
			continue
		}

		NodeListLock.Lock()
		for _, event := range watchResp.Events {
			parts := strings.Split(string(event.Kv.Key), "/")
			if len(parts) < 3 { continue }
			nodeName := parts[2]

			if _, ok := config.ExpectedNodeNames[nodeName]; !ok { continue }

			if event.Type == etcd.EventTypePut {
				nodeURL := string(event.Kv.Value)
				if _, exists := ActiveNodeURLs[nodeName]; !exists {
					log.Printf("%s[HEALER-Watcher] Node Joined: %s%s\n", config.Colors["GREEN"], nodeName, config.Colors["RESET"])
					ActiveNodeURLs[nodeName] = nodeURL
					if len(ActiveNodeURLs) >= config.K && !nodesReady {
						nodesReady = true
						close(NodesReadyEvent)
					}
				}
			} else if event.Type == etcd.EventTypeDelete {
				if _, exists := ActiveNodeURLs[nodeName]; exists {
					log.Printf("%s[HEALER-Watcher] Node Lost: %s%s\n", config.Colors["RED"], nodeName, config.Colors["RESET"])
					delete(ActiveNodeURLs, nodeName)
					if len(ActiveNodeURLs) < config.K && nodesReady {
						nodesReady = false
						NodesReadyEvent = make(chan struct{})
					}
				}
			}
		}
		NodeListLock.Unlock()
	}
}



// getDynamicNodes returns current healthy nodes separated by role.
func getDynamicNodes() ([]string, []string, error) {

	// Block until minimum nodes are ready
	select {
	case <-NodesReadyEvent:
	default:
		// If already closed, this passes. If nil (re-made), it blocks.
		// Simple non-blocking check if closed:
		select {
		case <-NodesReadyEvent:
		case <-time.After(time.Millisecond):
			return nil, nil, fmt.Errorf("waiting for nodes to be ready")
		}
	}

	NodeListLock.RLock()
	allNodes := make([]string, 0, len(ActiveNodeURLs))
	for _, url := range ActiveNodeURLs {
		allNodes = append(allNodes, url)
	}
	sort.Strings(allNodes)
	NodeListLock.RUnlock()

	replicaNodes := allNodes
	if len(allNodes) > 3 {
		replicaNodes = allNodes[:3]
	}
	ecNodes := allNodes

	if len(replicaNodes) < 3 {
		return nil, nil, fmt.Errorf("insufficient replica nodes (need 3)")
	}
	if len(ecNodes) < config.K+config.M {
		return nil, nil, fmt.Errorf("insufficient EC nodes (need %d)", config.K+config.M)
	}
	return replicaNodes, ecNodes, nil
}


func getKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}



// Phase 1: Rollback Logic 

// rollbackFailedTxn cleans up data from failed or stalled transactions.
func rollbackFailedTxn(
	ctx context.Context,
	etcdClient interfaces.IEtcdClient,
	storageOps interfaces.IStorageOps,
	keyName string,
	metadata map[string]interface{},
) error {
	log.Printf("%s[HEALER] Rolling back failed txn for key: %s...%s\n", config.Colors["RED"], keyName, config.Colors["RESET"])


	// Safety Check: Does valid metadata exist?
	metadataKey := fmt.Sprintf("metadata/%s", keyName)
	rangeResp, err := etcdClient.Get(ctx, metadataKey)
	if err != nil {
		return fmt.Errorf("rollback safety check failed: %v", err)
	}

	if len(rangeResp.Kvs) > 0 {
		log.Printf("%s[HEALER] Safety Check: Metadata exists. This is a stale txn log. Skipping delete to prevent data loss.%s\n", config.Colors["YELLOW"], config.Colors["RESET"])
		return nil
	}


	log.Printf("%s[HEALER] Safety Check Passed. Cleaning up zombie data...%s\n", config.Colors["GREEN"], config.Colors["RESET"])


	replicaNodes, ecNodes, err := getDynamicNodes()
	if err != nil {
		return fmt.Errorf("failed to get nodes for rollback: %v", err)
	}

	strategy, _ := metadata["strategy"].(string)

	switch config.StorageStrategy(strategy) {
	case config.StrategyReplication:
		storageOps.DeleteReplication(ctx, replicaNodes, keyName)
	case config.StrategyEC:
		storageOps.DeleteEC(ctx, ecNodes, metadata)
	case config.StrategyFieldHybrid:
		storageOps.DeleteFieldHybrid(ctx, replicaNodes, ecNodes, metadata)
	}


	// Double check to ensure no phantom metadata exists
	_, _ = etcdClient.Delete(ctx, metadataKey)

	log.Printf("%s[HEALER] Rollback complete for %s.%s\n", config.Colors["GREEN"], keyName, config.Colors["RESET"])
	return nil
}



// Phase 2: Self-Healing Logic

// auditReplicationKey checks if all replicas exist; if missing, copies from a healthy node.
func auditReplicationKey(
	ctx context.Context,
	httpClient interfaces.IHttpClient,
	replicaNodes []string,
	keyName string,
	metadata map[string]interface{},
) {
	keyToCheck := keyName
	strategy, _ := metadata["strategy"].(string)

	if config.StorageStrategy(strategy) == config.StrategyFieldHybrid {
		keyToCheck, _ = metadata["hot_key"].(string)
		if keyToCheck == "" {
			return
		}
	}

	log.Printf("%s[HEALER] Auditing Replica: %s (Target: %s)%s\n", config.Colors["CYAN"], keyName, keyToCheck, config.Colors["RESET"])

	// 1. Find a valid source
	var sourceData []byte
	var sourceNode string
	client := httpClient

	for _, nodeURL := range replicaNodes {
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", nodeURL, keyToCheck), nil)
		if err != nil { continue }
		resp, err := client.Do(req)
		if err != nil { continue }

		if resp.StatusCode == http.StatusOK {
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil {
				sourceData = data
				sourceNode = nodeURL
				break
			}
		} else {
			resp.Body.Close()
		}
	}

	if sourceData == nil {
		log.Printf("%s[HEALER] CRITICAL: All replicas lost for %s! Cannot repair.%s\n", config.Colors["RED"], keyToCheck, config.Colors["RESET"])
		return
	}

	// 2. Check and repair others
	for _, nodeURL := range replicaNodes {
		if nodeURL == sourceNode { continue }

		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", nodeURL, keyToCheck), nil)
		if err != nil { continue }
		resp, err := client.Do(req)
		if err != nil { continue }

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			log.Printf("%s[HEALER] Missing replica on %s. Repairing...%s\n", config.Colors["YELLOW"], nodeURL, config.Colors["RESET"])

			req, _ := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/store?key=%s", nodeURL, keyToCheck), bytes.NewReader(sourceData))
			req.Header.Set("Content-Type", "application/octet-stream")
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("%s[HEALER] Repair failed: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
			} else {
				resp.Body.Close()
				log.Printf("%s[HEALER] Repair successful: %s%s\n", config.Colors["GREEN"], nodeURL, config.Colors["RESET"])
			}
		} else {
			resp.Body.Close()
		}
	}
}



// auditECKey checks for missing shards and reconstructs them if K shards are available.
func auditECKey(
	ctx context.Context,
	httpClient interfaces.IHttpClient,
	ecDriver interfaces.IEcDriver,
	ecNodes []string,
	keyName string,
	metadata map[string]interface{},
) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s[HEALER] EC Repair Panic: %v%s\n", config.Colors["RED"], r, config.Colors["RESET"])
		}
	}()

	chunkPrefix := ""
	if cp, ok := metadata["cold_prefix"].(string); ok {
		chunkPrefix = cp
	} else if cp, ok := metadata["chunk_prefix"].(string); ok {
		chunkPrefix = cp
	}

	if chunkPrefix == "" { return }

	log.Printf("%s[HEALER] Auditing EC: %s%s\n", config.Colors["CYAN"], keyName, config.Colors["RESET"])

	missingIndexes := []int{}
	availableChunks := make([][]byte, len(ecNodes))
	healthyCount := 0

	var wg sync.WaitGroup
	var mutex sync.Mutex
	client := httpClient

	for i, nodeURL := range ecNodes {
		wg.Add(1)
		go func(index int, url string) {
			defer wg.Done()
			chunkKey := fmt.Sprintf("%s%d", chunkPrefix, index)
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", url, chunkKey), nil)
			if err != nil { return }

			resp, err := client.Do(req)
			if err != nil { return }
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				data, err := io.ReadAll(resp.Body)
				if err == nil {
					mutex.Lock()
					availableChunks[index] = data
					healthyCount++
					mutex.Unlock()
					return
				}
			}
			mutex.Lock()
			missingIndexes = append(missingIndexes, index)
			mutex.Unlock()
		}(i, nodeURL)
	}
	wg.Wait()

	if len(missingIndexes) == 0 { return } // All healthy

	log.Printf("%s[HEALER] Missing %d chunks for %s. Healthy: %d%s\n", config.Colors["YELLOW"], len(missingIndexes), keyName, healthyCount, config.Colors["RESET"])

	if healthyCount < config.K {
		log.Printf("%s[HEALER] CRITICAL: Not enough chunks to reconstruct (%d < %d). Data loss!%s\n", config.Colors["RED"], healthyCount, config.K, config.Colors["RESET"])
		return
	}

	log.Printf("%s[HEALER] Reconstructing missing chunks...%s\n", config.Colors["CYAN"], config.Colors["RESET"])

	if err := ecDriver.Reconstruct(availableChunks); err != nil {
		panic(fmt.Errorf("reconstruction failed: %v", err))
	}

	// Re-encode to ensure all parity shards are generated correctly
	var decodedData bytes.Buffer
	for i := 0; i < config.K; i++ {
		decodedData.Write(availableChunks[i])
	}
	allChunksNew, _ := ecDriver.Split(decodedData.Bytes())
	_ = ecDriver.Encode(allChunksNew)



	// Upload repaired chunks
	var uploadWg sync.WaitGroup
	for _, indexToFix := range missingIndexes {
		uploadWg.Add(1)
		go func(index int) {
			defer uploadWg.Done()
			nodeURL := ecNodes[index]
			chunkKey := fmt.Sprintf("%s%d", chunkPrefix, index)
			chunkData := allChunksNew[index]

			log.Printf("%s[HEALER] Uploading repaired chunk %d to %s...%s\n", config.Colors["GREEN"], index, nodeURL, config.Colors["RESET"])

			req, _ := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/store?key=%s", nodeURL, chunkKey), bytes.NewReader(chunkData))
			req.Header.Set("Content-Type", "application/octet-stream")
			client.Do(req)
		}(indexToFix)
	}
	uploadWg.Wait()
	log.Printf("%s[HEALER] Repair complete for %s%s\n", config.Colors["GREEN"], keyName, config.Colors["RESET"])
}




// Main Logic Loop

func mainLoop(
	ctx context.Context,
	etcdClient interfaces.IEtcdClient,
	httpClient interfaces.IHttpClient,
	ecDriver interfaces.IEcDriver,
	storageOps interfaces.IStorageOps,
) {
	log.Printf("%s--- Healer Service (%s) Started ---%s\n", config.Colors["MAGENTA"], MyID, config.Colors["RESET"])

	// Cast interface back to concrete type for concurrency session (limitation of etcd library)
	concreteEtcdClient, ok := etcdClient.(*etcd.Client)
	if !ok {
		log.Fatal("Healer requires a concrete etcd.Client for leader election")
	}

	go watchNodesTask(ctx, concreteEtcdClient)

	log.Printf("%s[HEALER] Waiting for storage nodes...%s\n", config.Colors["CYAN"], config.Colors["RESET"])
	select {
	case <-NodesReadyEvent:
		log.Printf("%s[HEALER] Nodes ready. Starting main loop.%s\n", config.Colors["CYAN"], config.Colors["RESET"])
	case <-ctx.Done():
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		isLeader := false
		var session *concurrency.Session
		var election *concurrency.Election

		func() {
			defer func() {
				if isLeader && election != nil {
					ctxRelease, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					election.Resign(ctxRelease)
					cancel()
					log.Printf("%s[HEALER] Leader resigned.%s\n", config.Colors["MAGENTA"], config.Colors["RESET"])
				}
				if r := recover(); r != nil {
					log.Printf("%s[HEALER] Panic in main loop: %v%s\n", config.Colors["RED"], r, config.Colors["RESET"])
				}
			}()

			// --- Phase 0: Leader Election ---
			var err error
			session, err = concurrency.NewSession(concreteEtcdClient, concurrency.WithTTL(60))
			if err != nil {
				log.Printf("Failed to create etcd session: %v", err)
				return
			}
			defer session.Close()

			election = concurrency.NewElection(session, config.EtcdHealerLock)
			log.Printf("%s[HEALER] Campaigning for leadership...%s\n", config.Colors["YELLOW"], config.Colors["RESET"])

			if err := election.Campaign(ctx, MyID); err != nil {
				log.Printf("Campaign failed: %v", err)
				return
			}

			isLeader = true
			log.Printf("%s[HEALER] I am the Leader. Starting audit.%s\n", config.Colors["GREEN"], config.Colors["RESET"])


			// --- Phase 1: Rollback Failed Transactions ---
			log.Println("[HEALER] Phase 1: Scanning pending transactions (WAL)...")
			rangeResp, err := etcdClient.Get(ctx, config.EtcdWALPrefix, etcd.WithPrefix())
			if err == nil {
				keysToDelete := [][]byte{}
				for _, kv := range rangeResp.Kvs {
					var logEntry map[string]interface{}
					if json.Unmarshal(kv.Value, &logEntry) != nil { continue }

					keyToFix, _ := logEntry["key_name"].(string)
					status, _ := logEntry["status"].(string)
					timestamp, _ := logEntry["timestamp"].(float64)

					// Check for timeout (> 5 mins)
					isTimeout := (status == "PENDING" && (time.Now().Unix()-int64(timestamp)) > 300)

					if status == "FAILED" || isTimeout {
						if err := rollbackFailedTxn(ctx, etcdClient, storageOps, keyToFix, logEntry); err == nil {
							keysToDelete = append(keysToDelete, kv.Key)
						}
					}
				}


				// Batch delete WAL entries
				if len(keysToDelete) > 0 {
					ops := []etcd.Op{}
					for _, k := range keysToDelete {
						ops = append(ops, etcd.OpDelete(string(k)))
					}
					etcdClient.Txn(ctx).Then(ops...).Commit()
					log.Printf("[HEALER] Phase 1: Cleaned %d failed txns.\n", len(keysToDelete))
				}
			}


			// --- Phase 2: Audit & Self-Heal ---
			log.Println("[HEALER] Phase 2: Auditing metadata integrity...")
			

			// Refresh node list snapshot
			NodeListLock.RLock()
			replicaNodes := sortedNodes(ActiveNodeURLs)
			if len(replicaNodes) > 3 { replicaNodes = replicaNodes[:3] }
			ecNodes := sortedNodes(ActiveNodeURLs)
			NodeListLock.RUnlock()

			rangeResp, err = etcdClient.Get(ctx, config.EtcdMetadataPrefix, etcd.WithPrefix())
			if err == nil {
				for _, kv := range rangeResp.Kvs {
					keyName := strings.Split(string(kv.Key), "/")[1]
					var metadata map[string]interface{}
					if json.Unmarshal(kv.Value, &metadata) != nil { continue }

					strategy, _ := metadata["strategy"].(string)
					switch config.StorageStrategy(strategy) {
					case config.StrategyReplication:
						auditReplicationKey(ctx, httpClient, replicaNodes, keyName, metadata)
					case config.StrategyEC:
						auditECKey(ctx, httpClient, ecDriver, ecNodes, keyName, metadata)
					case config.StrategyFieldHybrid:
						auditReplicationKey(ctx, httpClient, replicaNodes, keyName, metadata)
						auditECKey(ctx, httpClient, ecDriver, ecNodes, keyName, metadata)
					}
				}
			}
		}()


		// Sleep before next cycle
		log.Println("[HEALER] Cycle finished. Sleeping 60s...")
		select {
		case <-time.After(60 * time.Second):
		case <-ctx.Done():
			return
		}
	}
}


func sortedNodes(m map[string]string) []string {
	urls := make([]string, 0, len(m))
	for _, url := range m {
		urls = append(urls, url)
	}
	sort.Strings(urls)
	return urls
}


func main() {
	// 1. Initialize Dependencies
	etcdClient := etcdclient.GetClient()
	httpClient := httpclient.GetClient()
	ecDriver := ec.NewService()
	storageOps := storageops.NewService(httpClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Start Healer Loop
	mainLoop(ctx, etcdClient, httpClient, ecDriver, storageOps)

	log.Println("Healer stopping...")
	etcdclient.CloseClient()
}