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

// WriteTask represents an asynchronous write operation payload.
type WriteTask struct {
	Key  string
	Data []byte
}

// storageEngine handles raw file I/O operations with asynchronous write support.
type storageEngine struct {
	storageDir      string
	port            string
	nodeName        string
	totalOperations int64
	lock            sync.RWMutex

	// writeQueue buffers incoming write requests for background processing.
	writeQueue chan *WriteTask
}

// newStorageEngine initializes the storage directory and the async engine.
func newStorageEngine(port, nodeName, storageDir string) *storageEngine {
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage directory %s: %v", storageDir, err)
	}

	log.Printf("Storage Node (PID: %d, Port: %s) Started.", os.Getpid(), port)
	log.Printf("Data persistence path: %s", storageDir)

	engine := &storageEngine{
		storageDir: storageDir,
		port:       port,
		nodeName:   nodeName,
		// Initialize a buffered channel with a capacity of 5000 to handle burst traffic.
		writeQueue: make(chan *WriteTask, 5000),
	}

	// Start the background I/O worker to consume tasks.
	go engine.startIoWorker()

	return engine
}

// startIoWorker consumes write tasks from the queue and performs blocking disk I/O.
func (s *storageEngine) startIoWorker() {
	log.Println("[Async IO] Worker started. Waiting for tasks...")
	for task := range s.writeQueue {
		filePath, err := s._getSafePath(task.Key)
		if err != nil {
			log.Printf("[Async IO] Error resolving path for key %s: %v", task.Key, err)
			continue
		}

		// Perform the actual blocking disk write operation.
		if err := os.WriteFile(filePath, task.Data, 0644); err != nil {
			log.Printf("[Async IO] Disk Write Failed for %s: %v", filePath, err)
		}

		// Update metrics
		s.lock.Lock()
		s.totalOperations++
		s.lock.Unlock()
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

// store queues the write task for asynchronous processing.
func (s *storageEngine) store(key string, data []byte) (int, error) {
	_, err := s._getSafePath(key)
	if err != nil {
		return 0, err
	}

	task := &WriteTask{
		Key:  key,
		Data: data,
	}

	// Non-blocking enqueue
	select {
	case s.writeQueue <- task:
		return len(data), nil
	default:
		log.Printf("[Async IO] Queue Full! Dropping request for key: %s", key)
		return 0, fmt.Errorf("storage node overloaded (queue full)")
	}
}

// retrieve reads data from disk synchronously.
func (s *storageEngine) retrieve(key string) ([]byte, error) {
	filePath, err := s._getSafePath(key)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		log.Printf("Error reading file %s: %v", filePath, err)
		return nil, err
	}
	return data, nil
}

// delete removes data from disk synchronously.
func (s *storageEngine) delete(key string) (bool, error) {
	filePath, err := s._getSafePath(key)
	if err != nil {
		return false, err
	}

	err = os.Remove(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		log.Printf("Error deleting file %s: %v", filePath, err)
		return false, err
	}
	return true, nil
}

// getInfo returns current statistics.
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
		"total_keys":        totalKeys,
		"total_size":        totalSize,
		"total_operations":  ops,
		"storage_path":      s.storageDir,
		"write_queue_depth": len(s.writeQueue),
		"write_queue_cap":   cap(s.writeQueue),
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
				etcdClient.Revoke(context.Background(), leaseID)
				log.Printf("[%s] Shutting down, lease revoked.", nodeName)
				return

			case ka, ok := <-keepAliveChan:
				if !ok {
					log.Printf("[%s] Warning: KeepAlive channel closed. Re-registering...", nodeName)
					break // Break inner loop to re-register
				}
				_ = ka
			}
		}
	}
}

// --- Main Entry Point ---

func main() {
	_ = config.Colors
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	nodePort := os.Getenv("NODE_PORT")
	nodeName := os.Getenv("NODE_NAME")
	storageDir := os.Getenv("STORAGE_DIR")
	if nodePort == "" || nodeName == "" || storageDir == "" {
		log.Fatal("Error: NODE_PORT, NODE_NAME, and STORAGE_DIR must be set.")
	}

	etcdClient := etcd.GetClient()
	if etcdClient == nil {
		log.Fatalf("Failed to connect to Etcd. Cannot start Storage Node.")
	}
	defer etcd.CloseClient()

	storage := newStorageEngine(nodePort, nodeName, storageDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	internalURL := fmt.Sprintf("http://%s:%s", nodeName, nodePort)
	go registerAndHeartbeat(ctx, etcdClient, nodeName, internalURL)

	// 5. Start Gin Server
	gin.SetMode(gin.ReleaseMode)
	
	// [CRITICAL CHANGE] Use gin.New() instead of gin.Default()
	// gin.Default() uses Logger() middleware which prints to stdout for every request.
	// This synchronous I/O kills performance in high throughput benchmarks.
	router := gin.New()
	
	// Only add Recovery middleware to prevent crashes
	router.Use(gin.Recovery()) 

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
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
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