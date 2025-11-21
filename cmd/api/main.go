package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	etcd "go.etcd.io/etcd/client/v3"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	etcdclient "hybrid_distributed_store/internal/etcd"
	"hybrid_distributed_store/internal/httpclient"
	"hybrid_distributed_store/internal/monitoringservice"
	"hybrid_distributed_store/internal/readservice"
	"hybrid_distributed_store/internal/storageops"
	"hybrid_distributed_store/internal/utils"
	"hybrid_distributed_store/internal/writeservice"
)

// --- Service Discovery Globals ---
var (
	ActiveNodeURLs  = make(map[string]string)
	NodeListLock    = &sync.RWMutex{}
	NodesReadyEvent = make(chan struct{})
	nodesReady      = false
)


// watchNodesTask monitors Etcd for storage node registration/deregistration.
func watchNodesTask(ctx context.Context, etcdClient *etcd.Client) {
	keyPrefix := "nodes/health/"
	log.Printf("%s[API] Service Discovery started. Watching prefix: '%s'%s\n", config.Colors["CYAN"], keyPrefix, config.Colors["RESET"])


	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s[API-Watcher] PANIC: %v%s\n", config.Colors["RED"], r, config.Colors["RESET"])
			log.Println(string(debug.Stack()))
		}
		log.Printf("%s[API-Watcher] Service Discovery stopped.%s\n", config.Colors["RED"], config.Colors["RESET"])
	}()


	// 1. Initial Fetch
	rangeResp, err := etcdClient.Get(ctx, keyPrefix, etcd.WithPrefix())
	if err != nil {
		log.Printf("%s[API] Initial node fetch failed: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
	} else {
		NodeListLock.Lock()
		ActiveNodeURLs = make(map[string]string)
		for _, kv := range rangeResp.Kvs {
			parts := strings.Split(string(kv.Key), "/")
			if len(parts) < 3 {
				continue
			}
			nodeName := parts[2]
			nodeURL := string(kv.Value)
			if _, ok := config.ExpectedNodeNames[nodeName]; ok {
				ActiveNodeURLs[nodeName] = nodeURL
			}
		}
		log.Printf("%s[API] Initial Nodes: %v%s\n", config.Colors["CYAN"], getKeys(ActiveNodeURLs), config.Colors["RESET"])
		
		// If we have enough nodes initially, signal readiness
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
			log.Printf("%s[API] Watch error: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
			time.Sleep(2 * time.Second)
			continue
		}

		NodeListLock.Lock()
		for _, event := range watchResp.Events {
			parts := strings.Split(string(event.Kv.Key), "/")
			if len(parts) < 3 {
				continue
			}
			nodeName := parts[2]
			
			if _, ok := config.ExpectedNodeNames[nodeName]; !ok {
				continue
			}

			if event.Type == etcd.EventTypePut {
				nodeURL := string(event.Kv.Value)
				if _, exists := ActiveNodeURLs[nodeName]; !exists {
					log.Printf("%s[API] New Node Detected: %s%s\n", config.Colors["GREEN"], nodeName, config.Colors["RESET"])
					ActiveNodeURLs[nodeName] = nodeURL
					if len(ActiveNodeURLs) >= config.K && !nodesReady {
						nodesReady = true
						close(NodesReadyEvent)
					}
				}
			} else if event.Type == etcd.EventTypeDelete {
				if _, exists := ActiveNodeURLs[nodeName]; exists {
					log.Printf("%s[API] Node Lost: %s%s\n", config.Colors["RED"], nodeName, config.Colors["RESET"])
					delete(ActiveNodeURLs, nodeName)
					// If nodes drop below minimum, reset readiness? (Optional logic, strict mode below)
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




// getDynamicNodes ensures enough nodes are available for operations.
func getDynamicNodes(c *gin.Context) ([]string, []string, error) {
	select {
	case <-NodesReadyEvent:
	case <-time.After(30 * time.Second):
		log.Printf("%s[API] Timeout waiting for storage nodes%s\n", config.Colors["RED"], config.Colors["RESET"])
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Service unavailable: Storage node registration timeout"})
		return nil, nil, fmt.Errorf("service unavailable")
	}

	NodeListLock.RLock()
	allNodes := make([]string, 0, len(ActiveNodeURLs))
	for _, url := range ActiveNodeURLs {
		allNodes = append(allNodes, url)
	}
	sort.Strings(allNodes)
	NodeListLock.RUnlock()


	// Simple partitioning strategy: First 3 are replicas, All are EC candidates

	replicaNodes := allNodes
	if len(allNodes) > 3 {
		replicaNodes = allNodes[:3]
	}
	ecNodes := allNodes

	if len(replicaNodes) < 3 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Service unavailable: Insufficient replica nodes (need 3)"})
		return nil, nil, fmt.Errorf("service unavailable")
	}
	if len(ecNodes) < config.K+config.M {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": fmt.Sprintf("Service unavailable: Insufficient EC nodes (need %d)", config.K+config.M)})
		return nil, nil, fmt.Errorf("service unavailable")
	}

	return replicaNodes, ecNodes, nil
}



// PanicRecoveryMiddleware handles unhandled panics to prevent server crash.
func PanicRecoveryMiddleware(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s[API] PANIC: %v%s\n", config.Colors["RED"], r, config.Colors["RESET"])
			debug.PrintStack()
			
			var errorMsg string
			if err, ok := r.(error); ok {
				errorMsg = err.Error()
			} else {
				errorMsg = fmt.Sprintf("%v", r)
			}
			
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":  "Internal Server Error",
				"detail": errorMsg,
			})
			c.Abort()
		}
	}()
	c.Next()
}



func getKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}



func main() {
	log.Printf("%sAPI Gateway (PID: %d) Starting...%s\n", config.Colors["GREEN"], os.Getpid(), config.Colors["RESET"])


	// 1. Initialize Services
	etcdClient := etcdclient.GetClient()
	httpClient := httpclient.GetClient()
	
	utilsSvc := utils.NewService()
	ecDriver := ec.NewService()
	storageOpsSvc := storageops.NewService(httpClient)
	
	readSvc := readservice.NewService(httpClient, ecDriver, utilsSvc)
	writeSvc := writeservice.NewService(etcdClient, httpClient, readSvc, ecDriver, utilsSvc)


	// 2. Start Service Discovery
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchNodesTask(ctx, etcdClient)


	// 3. Setup Router
	router := gin.New()
	router.Use(gin.Logger())
	router.Use(PanicRecoveryMiddleware)


	// --- Write Endpoint ---
	router.POST("/write", func(c *gin.Context) {
		start := time.Now()
		key := c.Query("key")
		strategy := config.StorageStrategy(c.DefaultQuery("strategy", string(config.StrategyReplication)))
		hotOnlyStr := c.Query("hot_only")
		isHotOnly := strings.ToLower(hotOnlyStr) == "true"


		replicaNodes, ecNodes, err := getDynamicNodes(c)
		if err != nil {
			return
		}


		var result map[string]interface{}
		var dataDict map[string]interface{}

		contentType := c.GetHeader("Content-Type")
		if !strings.HasPrefix(contentType, "application/json") {
			c.JSON(http.StatusUnsupportedMediaType, gin.H{
				"error":  "Invalid Content-Type",
				"detail": "Must be application/json",
			})
			return
		}

		if err := c.ShouldBindJSON(&dataDict); err != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Invalid JSON body"})
			return
		}
		if (dataDict == nil || len(dataDict) == 0) && c.Request.ContentLength > 0 {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "JSON body cannot be empty"})
			return
		}

		switch strategy {
		case config.StrategyReplication, config.StrategyEC:
			bodyBytes, err := utilsSvc.Serialize(dataDict)
			if err != nil {
				panic(fmt.Errorf("JSON serialization failed: %v", err))
			}
			if strategy == config.StrategyReplication {
				result, err = writeSvc.WriteReplication(c.Request.Context(), replicaNodes, key, bodyBytes)
			} else {
				result, err = writeSvc.WriteEC(c.Request.Context(), ecNodes, key, bodyBytes)
			}
		case config.StrategyFieldHybrid:
			result, err = writeSvc.WriteFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, key, dataDict, isHotOnly)
		default:
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Invalid strategy"})
			return
		}

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		result["status"] = "ok"
		result["strategy"] = string(strategy)
		result["key"] = key
		result["latency_ms"] = time.Since(start).Milliseconds()
		c.JSON(http.StatusOK, result)
	})


	// --- Read Endpoint ---
	router.GET("/read/:key", func(c *gin.Context) {
		key := c.Param("key")
		replicaNodes, ecNodes, err := getDynamicNodes(c)
		if err != nil {
			return
		}

		etcdKey := fmt.Sprintf("metadata/%s", key)
		rangeResp, err := etcdClient.Get(c.Request.Context(), etcdKey)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Etcd query failed: %v", err)})
			return
		}
		if len(rangeResp.Kvs) == 0 {
			c.JSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Metadata not found for key '%s'", key)})
			return
		}

		var metadata map[string]interface{}
		if err := json.Unmarshal(rangeResp.Kvs[0].Value, &metadata); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Metadata parse failed: %v", err)})
			return
		}

		strategyStr, _ := metadata["strategy"].(string)
		switch config.StorageStrategy(strategyStr) {
		case config.StrategyReplication, config.StrategyEC:
			var dataBytes []byte
			var errRead error
			if config.StorageStrategy(strategyStr) == config.StrategyReplication {
				dataBytes, errRead = readSvc.ReadReplication(c.Request.Context(), replicaNodes, key)
			} else {
				dataBytes, errRead = readSvc.ReadEC(c.Request.Context(), ecNodes, metadata)
			}
			
			if errRead != nil {
				c.JSON(http.StatusNotFound, gin.H{"detail": errRead.Error()})
				return
			}
			
			dataDict, errDes := utilsSvc.Deserialize(dataBytes)
			if errDes != nil {
				c.Data(http.StatusOK, "application/octet-stream", dataBytes)
			} else {
				c.JSON(http.StatusOK, dataDict)
			}

		case config.StrategyFieldHybrid:
			dataDict, err := readSvc.ReadFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, metadata)
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{"detail": err.Error()})
				return
			}
			c.JSON(http.StatusOK, dataDict)

		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Unknown strategy in metadata: %s", strategyStr)})
		}
	})


	// --- Delete Endpoint ---
	router.DELETE("/delete/:key", func(c *gin.Context) {
		start := time.Now()
		key := c.Param("key")
		replicaNodes, ecNodes, err := getDynamicNodes(c)
		if err != nil {
			return
		}

		result := make(map[string]interface{})
		strategyStr := "N/A"
		etcdKey := fmt.Sprintf("metadata/%s", key)

		rangeResp, err := etcdClient.Get(c.Request.Context(), etcdKey)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Etcd query failed: %v", err)})
			return
		}

		metadata := make(map[string]interface{})
		if len(rangeResp.Kvs) > 0 {
			if err := json.Unmarshal(rangeResp.Kvs[0].Value, &metadata); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Metadata parse failed: %v", err)})
				return
			}
		}

		if len(metadata) > 0 {
			// Case A: Normal Delete
			log.Printf("%sDelete [Index Found] key=%s%s\n", config.Colors["RED"], key, config.Colors["RESET"])
			strategyStr, _ = metadata["strategy"].(string)

			var hotCount, coldCount int
			var delErr error

			switch config.StorageStrategy(strategyStr) {
			case config.StrategyReplication:
				hotCount, delErr = storageOpsSvc.DeleteReplication(c.Request.Context(), replicaNodes, key)
				result["nodes_deleted"] = hotCount
			case config.StrategyEC:
				coldCount, delErr = storageOpsSvc.DeleteEC(c.Request.Context(), ecNodes, metadata)
				result["chunks_deleted"] = coldCount
			case config.StrategyFieldHybrid:
				hotCount, coldCount, delErr = storageOpsSvc.DeleteFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, metadata)
				result["hot_nodes_deleted"] = hotCount
				result["cold_chunks_deleted"] = coldCount
			default:
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Unknown strategy: %s", strategyStr)})
				return
			}

			if delErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Data plane deletion failed: %v", delErr)})
				return
			}

			_, err = etcdClient.Delete(c.Request.Context(), etcdKey)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Metadata deletion failed: %v", err)})
				return
			}

		} else {
			// Case B: Blind Delete (Cleanup Zombie Data)
			log.Printf("%sDelete [Index Not Found] key=%s. Executing Blind Delete...%s\n", config.Colors["YELLOW"], key, config.Colors["RESET"])
			strategyStr = "blind_delete"

			blindMetadata := map[string]interface{}{"key_name": key}
			storageOpsSvc.DeleteReplication(c.Request.Context(), replicaNodes, key)
			storageOpsSvc.DeleteEC(c.Request.Context(), ecNodes, blindMetadata)
			storageOpsSvc.DeleteFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, blindMetadata)

			result["detail"] = "key_not_found_or_zombie_cleaned"
		}

		result["status"] = "ok"
		result["strategy"] = strategyStr
		result["key"] = key
		result["latency_ms"] = time.Since(start).Milliseconds()
		c.JSON(http.StatusOK, result)
	})


	// --- Monitoring Endpoints ---
	router.GET("/node_status", func(c *gin.Context) {
		var currentNodes []string
		NodeListLock.RLock()
		for _, url := range ActiveNodeURLs {
			currentNodes = append(currentNodes, url)
		}
		NodeListLock.RUnlock()
		
		if len(currentNodes) == 0 {
			c.JSON(http.StatusOK, gin.H{})
			return
		}
		
		statusMap, err := monitoringservice.FetchNodeStatus(c.Request.Context(), currentNodes)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to fetch node status: %v", err)})
			return
		}
		c.JSON(http.StatusOK, statusMap)
	})


	router.GET("/storage_usage", func(c *gin.Context) {
		var currentNodes []string
		NodeListLock.RLock()
		for _, url := range ActiveNodeURLs {
			currentNodes = append(currentNodes, url)
		}
		NodeListLock.RUnlock()

		if len(currentNodes) == 0 {
			c.JSON(http.StatusOK, gin.H{"total_system_size": 0, "active_nodes_with_data": 0, "total_nodes_queried": 0})
			return
		}

		usageMap, err := monitoringservice.FetchStorageUsage(c.Request.Context(), currentNodes)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to fetch storage usage: %v", err)})
			return
		}
		c.JSON(http.StatusOK, usageMap)
	})


	router.GET("/health", func(c *gin.Context) {
		hostname, _ := os.Hostname()
		c.JSON(http.StatusOK, gin.H{
			"status":   "healthy",
			"service":  "api_gateway",
			"hostname": hostname,
		})
	})

	
	// 4. Start Server
	log.Println("[API] Starting Gin Server on 0.0.0.0:8000...")
	if err := router.Run("0.0.0.0:8000"); err != nil {
		log.Fatalf("Gin Server failed to start: %v", err)
	}
}