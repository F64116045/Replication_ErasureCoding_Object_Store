package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"

	"hybrid_distributed_store/internal/config"
	etcd "hybrid_distributed_store/internal/etcd"
)

// StorageEngine handles raw file I/O operations.
type storageEngine struct {
	storageDir      string
	port            string
	nodeName        string
	totalOperations int64
	lock            sync.RWMutex
}

// newStorageEngine initializes the storage directory and engine.
func newStorageEngine(port, nodeName, storageDir string) *storageEngine {
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage directory %s: %v", storageDir, err)
	}

	log.Printf("Storage Node (PID: %d, Port: %s) Started.", os.Getpid(), port)
	log.Printf("Data persistence path: %s", storageDir)

	return &storageEngine{
		storageDir: storageDir,
		port:       port,
		nodeName:   nodeName,
	}
}

// _getSafePath prevents directory traversal attacks.
func (s *storageEngine) _getSafePath(key string) (string, error) {
	safeKey := filepath.Clean(key)
	if strings.Contains(safeKey, "..") || strings.HasPrefix(safeKey, "/") {
		return "", fmt.Errorf("invalid key: %s", key)
	}
	return filepath.Join(s.storageDir, safeKey), nil
}

// store writes data to disk.
func (s *storageEngine) store(key string, data []byte) (int, error) {
	filePath, err := s._getSafePath(key)
	if err != nil {
		return 0, err
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		log.Printf("Error writing file %s: %v", filePath, err)
		return 0, err
	}

	s.lock.Lock()
	s.totalOperations++
	s.lock.Unlock()

	return len(data), nil
}

// retrieve reads data from disk.
func (s *storageEngine) retrieve(key string) ([]byte, error) {
	filePath, err := s._getSafePath(key)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // Return nil if not found
		}
		log.Printf("Error reading file %s: %v", filePath, err)
		return nil, err
	}
	return data, nil
}

// delete removes data from disk.
func (s *storageEngine) delete(key string) (bool, error) {
	filePath, err := s._getSafePath(key)
	if err != nil {
		return false, err
	}

	err = os.Remove(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // File didn't exist, return false
		}
		log.Printf("Error deleting file %s: %v", filePath, err)
		return false, err
	}
	return true, nil
}

// getInfo returns statistics about the storage node.
func (s *storageEngine) getInfo() (map[string]interface{}, error) {
	s.lock.RLock()
	ops := s.totalOperations
	s.lock.RUnlock()

	var totalKeys int64 = 0
	var totalSize int64 = 0

	entries, err := os.ReadDir(s.storageDir)
	if err != nil {
		log.Printf("Error scanning storage dir: %v", err)
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			info, err := entry.Info()
			if err == nil {
				totalKeys++
				totalSize += info.Size()
			}
		}
	}

	return map[string]interface{}{
		"total_keys":       totalKeys,
		"total_size":       totalSize,
		"total_operations": ops,
		"storage_path":     s.storageDir,
	}, nil
}

// --- Service Registration & Heartbeat ---

func registerAndHeartbeat(ctx context.Context, etcdClient *clientv3.Client, nodeName, nodeURL string) {
	log.Printf("[%s] Starting Service Registration...", nodeName)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s[%s] Heartbeat PANIC: %v%s\n", config.Colors["RED"], nodeName, r, config.Colors["RESET"])
			log.Println(string(debug.Stack()))
		}
		log.Printf("%s[%s] Heartbeat stopped.%s\n", config.Colors["RED"], nodeName, config.Colors["RESET"])
	}()

	for {
		var leaseID clientv3.LeaseID = 0

	tryLease:
		for {
			leaseResp, err := etcdClient.Grant(ctx, 10) // 10 seconds TTL
			if err != nil {
				log.Printf("[%s] Warning: Failed to grant lease: %v. Retrying in 2s...", nodeName, err)
				time.Sleep(2 * time.Second)
				continue tryLease
			}
			leaseID = leaseResp.ID
			break
		}

		key := fmt.Sprintf("nodes/health/%s", nodeName)
		_, err := etcdClient.Put(ctx, key, nodeURL, clientv3.WithLease(leaseID))
		if err != nil {
			log.Printf("[%s] Warning: Failed to register: %v. Retrying in 2s...", nodeName, err)
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("[%s] Registered successfully (LeaseID: %x). Starting KeepAlive.", nodeName, leaseID)

		keepAliveChan, err := etcdClient.KeepAlive(ctx, leaseID)
		if err != nil {
			log.Printf("[%s] Warning: Failed to start KeepAlive: %v. Retrying in 2s...", nodeName, err)
			time.Sleep(2 * time.Second)
			continue
		}

		for {
			select {
			case <-ctx.Done():
				// Context cancelled, revoke lease immediately
				etcdClient.Revoke(context.Background(), leaseID)
				log.Printf("[%s] Shutting down, lease revoked.", nodeName)
				return

			case ka, ok := <-keepAliveChan:
				if !ok {
					log.Printf("[%s] Warning: KeepAlive channel closed. Re-registering...", nodeName)
					break // Break inner loop to re-register
				}
				// Heartbeat received
				_ = ka
			}
		}
	}
}

// --- Main Entry Point ---

func main() {
	_ = config.Colors
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// 1. Load Env Vars
	nodePort := os.Getenv("NODE_PORT")
	nodeName := os.Getenv("NODE_NAME")
	storageDir := os.Getenv("STORAGE_DIR")
	if nodePort == "" || nodeName == "" || storageDir == "" {
		log.Fatal("Error: NODE_PORT, NODE_NAME, and STORAGE_DIR must be set.")
	}

	// 2. Initialize Etcd Client
	etcdClient := etcd.GetClient()
	if etcdClient == nil {
		log.Fatalf("Failed to connect to Etcd. Cannot start Storage Node.")
	}
	defer etcd.CloseClient()

	// 3. Initialize Storage Engine
	storage := newStorageEngine(nodePort, nodeName, storageDir)

	// 4. Start Registration & Heartbeat (Background)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Construct the internal URL that other services (API/Healer) will use to call this node
	// format: http://storage_node_1:8001
	internalURL := fmt.Sprintf("http://%s:%s", nodeName, nodePort)
	go registerAndHeartbeat(ctx, etcdClient, nodeName, internalURL)

	// 5. Start Gin Server
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	// 6. Bind Routes
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "healthy",
			"service": fmt.Sprintf("storage_node_%s", storage.port),
		})
	})

	router.GET("/info", func(c *gin.Context) {
		info, err := storage.getInfo()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, info)
	})

	router.POST("/store", func(c *gin.Context) {
		key := c.Query("key")
		if key == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing 'key' query parameter"})
			return
		}

		data, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read body"})
			return
		}

		size, err := storage.store(key, data)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		info, _ := storage.getInfo()
		c.JSON(http.StatusOK, gin.H{
			"status":     "ok",
			"key":        key,
			"size":       size,
			"total_keys": info["total_keys"],
		})
	})

	router.GET("/retrieve/:key", func(c *gin.Context) {
		key := c.Param("key")
		data, err := storage.retrieve(key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if data == nil {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Key not found"})
			return
		}
		c.Data(http.StatusOK, "application/octet-stream", data)
	})

	router.DELETE("/delete/:key", func(c *gin.Context) {
		key := c.Param("key")
		deleted, err := storage.delete(key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !deleted {
			c.JSON(http.StatusOK, gin.H{"status": "ok", "key": key, "detail": "not_found"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "key": key, "message": "deleted"})
	})

	// 7. Start Server
	listenAddr := "0.0.0.0:" + nodePort
	log.Printf("[%s] Gin Server starting on %s", nodeName, listenAddr)
	if err := router.Run(listenAddr); err != nil {
		log.Fatalf("[%s] Critical Error: Gin failed to start: %v", nodeName, err)
	}
}