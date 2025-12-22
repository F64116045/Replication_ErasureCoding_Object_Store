package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
	"hybrid_distributed_store/internal/mq"
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

// runWithLeaderElection handles the leadership campaign
func (h *HealerService) runWithLeaderElection(ctx context.Context, cfg Config) error {
	// 1. Create Etcd Session (TTL handles heartbeat)
	session, err := concurrency.NewSession(h.etcd, concurrency.WithTTL(15))
	if err != nil {
		return fmt.Errorf("failed to create etcd session: %w", err)
	}
	defer session.Close()

	election := concurrency.NewElection(session, cfg.LeaderKey)
	log.Println("[Election] Campaigning for leadership...")

	// 2. Campaign (Block until we become leader)
	if err := election.Campaign(ctx, "healer-node-"+os.Getenv("HOSTNAME")); err != nil {
		if err == context.Canceled {
			return nil
		}
		return fmt.Errorf("campaign failed: %w", err)
	}

	log.Println("===================================================")
	log.Printf("%s[Election] WINNER! I am the Leader now.%s", config.Colors["GREEN"], config.Colors["RESET"])
	log.Println("===================================================")

	// 3. Run Tasks (Polling Loop + WAL Consumer)
	var wg sync.WaitGroup
	wg.Add(2)

	// Task A: Maintenance (Polling)
	go func() {
		defer wg.Done()
		h.runControlLoop(ctx, cfg)
	}()

	// Task B: Crash Recovery (Consumer)
	go func() {
		defer wg.Done()
		h.runWALConsumer(ctx, cfg)
	}()

	// Wait for shutdown signal or session loss
	select {
	case <-ctx.Done():
		log.Println("[Election] Resigning leadership...")
		ctxResign, cancelResign := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelResign()
		election.Resign(ctxResign)
		return nil
	case <-session.Done():
		return fmt.Errorf("lost etcd session ownership")
	}
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

// getSortedNodes returns a deterministic list of nodes
func (h *HealerService) getSortedNodes() ([]string, []string) {
	h.nodeLock.RLock()
	defer h.nodeLock.RUnlock()

	allNodes := make([]string, 0, len(h.activeNodeURLs))
	for _, url := range h.activeNodeURLs {
		allNodes = append(allNodes, url)
	}
	sort.Strings(allNodes)

	replicaNodes := allNodes
	if len(allNodes) > 3 {
		replicaNodes = allNodes[:3]
	}
	return replicaNodes, allNodes
}

// --- HTTP Helpers ---

func (h *HealerService) checkFileExists(nodeURL, key string) bool {
	url := fmt.Sprintf("%s/retrieve/%s", nodeURL, key)
	resp, err := h.httpClient.Head(url)
	if err != nil {
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