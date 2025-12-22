package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"hybrid_distributed_store/internal/ec"
	etcdclient "hybrid_distributed_store/internal/etcd"
	"hybrid_distributed_store/internal/httpclient"
	"hybrid_distributed_store/internal/mq"
	"hybrid_distributed_store/internal/utils"
)

// Config holds the configuration for the Healer service
// 這個 Struct 會被 poller.go 和 consumer.go 共用
type Config struct {
	CheckInterval time.Duration // Interval for the polling loop
	LeaderKey     string        // Etcd key for leader election
	RetryDelay    time.Duration // Time to wait before verifying a WAL transaction (Grace Period)
}

func main() {
	log.Println("[Healer] Starting Production-Grade Recovery Service (Hybrid Mode)...")

	// 1. Initialize Dependencies
	etcdCl := etcdclient.GetClient()
	mqCl := mq.NewClient(true) // Consumer Mode
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
	// Healer needs to know valid storage nodes to perform repairs
	go service.watchNodes(context.Background())

	// 3. Setup Configuration
	cfg := Config{
		CheckInterval: 30 * time.Second,
		LeaderKey:     "/healer/leader",
		RetryDelay:    10 * time.Second, // Wait 10s for API Gateway to finish writing
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

	// 5. Start Service with Leader Election
	// This blocks until the service is stopped or crashes
	if err := service.runWithLeaderElection(ctx, cfg); err != nil {
		log.Fatalf("Healer service failed: %v", err)
	}

	log.Println("Healer Service stopped successfully.")
}