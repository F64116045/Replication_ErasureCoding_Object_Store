package config

import (
	"os"
	"strings"
)

// Colors for terminal output
var Colors = map[string]string{
	"GREEN":   "\033[92m",
	"RED":     "\033[91m",
	"YELLOW":  "\033[93m",
	"CYAN":    "\033[96m",
	"MAGENTA": "\033[95m",
	"BOLD":    "\033[1m",
	"RESET":   "\033[0m",
}

// Erasure Coding parameters (Reed-Solomon)
const (
	K = 4 // Number of Data Shards
	M = 2 // Number of Parity Shards
)

// StorageStrategy defines the method used for data persistence
type StorageStrategy string

const (
	StrategyReplication StorageStrategy = "replication"
	StrategyEC          StorageStrategy = "ec"
	StrategyFieldHybrid StorageStrategy = "field_hybrid"
)

// HotFields defines specific fields that are frequently updated
// Used for the FieldHybrid strategy logic
var HotFields = map[string]bool{
	"like_count":      true,
	"view_count":      true,
	"inventory_count": true,
	"update_count":    true,
}

// Etcd related constants for keys and prefixes
const (
	// EtcdNodePrefix: Prefix for storage node health registration
	// Example: "nodes/health/storage_node_1"
	EtcdNodePrefix = "nodes/health/"

	// EtcdMetadataPrefix: Prefix for storing object metadata
	// Example: "metadata/my_key"
	EtcdMetadataPrefix = "metadata/"

	// EtcdWALPrefix: Prefix for Write-Ahead Log entries during transactions
	// Example: "txn/uuid-123"
	EtcdWALPrefix = "txn/"

	// EtcdHealerLock: Key used for healer service leader election
	EtcdHealerLock = "healer_leader_lock"
)

// ExpectedNodeNames stores the set of valid storage node identifiers
var ExpectedNodeNames = map[string]bool{}

func init() {
	// Load expected node names from environment variable or use default fallback
	names := os.Getenv("NODE_NAMES_CSV")
	if names == "" {
		names = "storage_node_1,storage_node_2,storage_node_3,storage_node_4,storage_node_5,storage_node_6"
	}

	for _, name := range strings.Split(names, ",") {
		ExpectedNodeNames[name] = true
	}
}