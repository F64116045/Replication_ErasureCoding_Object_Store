package interfaces

import (
	"context"
	"net/http"

	etcd "go.etcd.io/etcd/client/v3"
)



// IEtcdClient defines the minimal functionality required from an etcd client.
// It abstracts the underlying etcd implementation for easier testing and mocking.
type IEtcdClient interface {
	Put(ctx context.Context, key, val string, opts ...etcd.OpOption) (*etcd.PutResponse, error)
	Get(ctx context.Context, key string, opts ...etcd.OpOption) (*etcd.GetResponse, error)
	Txn(ctx context.Context) etcd.Txn
	Delete(ctx context.Context, key string, opts ...etcd.OpOption) (*etcd.DeleteResponse, error)
}

// IHttpClient defines the minimal functionality required from an HTTP client.
type IHttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}


// IReadService defines the read operations for different storage strategies.
// It handles logic for Replication, Erasure Coding , and Hybrid storage.
type IReadService interface {
	// CheckFirstWrite verifies if the hot key exists in the replica nodes.
	CheckFirstWrite(ctx context.Context, replicaNodes []string, hotKey string) (bool, error)
	
	// GetExistingColdFields retrieves cold data fields from EC nodes based on metadata.
	GetExistingColdFields(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (map[string]interface{}, error)

	ReadReplication(ctx context.Context, replicaNodes []string, key string) ([]byte, error)
	ReadEC(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error)
	ReadFieldHybrid(ctx context.Context, replicaNodes, ecNodes []string, metadata map[string]interface{}) (map[string]interface{}, error)
}

// IEcDriver defines the interface for Erasure Coding algorithms.
// Implementing classes should handle Reed-Solomon encoding/decoding.
type IEcDriver interface {
	// Split divides the data into data shards.
	Split(data []byte) ([][]byte, error)
	
	// Encode generates parity shards from data shards.
	Encode(shards [][]byte) error
	
	// Reconstruct recovers missing shards using the available ones.
	Reconstruct(shards [][]byte) error
}

// IUtilsSvc defines helper utilities for data processing.
type IUtilsSvc interface {
	// SeparateHotColdFields splits data into hot (frequently accessed) and cold (infrequently accessed) maps.
	SeparateHotColdFields(data map[string]interface{}) (map[string]interface{}, map[string]interface{})
	
	Serialize(data map[string]interface{}) ([]byte, error)
	Deserialize(data []byte) (map[string]interface{}, error)
	
	MapsAreEqual(map1, map2 map[string]interface{}) bool
	
	// MergeHotColdFields combines hot and cold maps back into a single structure.
	MergeHotColdFields(hotFields, coldFields map[string]interface{}) map[string]interface{}
}

// IStorageOps defines deletion operations across different storage strategies.
type IStorageOps interface {
	DeleteReplication(ctx context.Context, replicaNodes []string, key string) (int, error)
	DeleteEC(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (int, error)
	DeleteFieldHybrid(ctx context.Context, replicaNodes, ecNodes []string, metadata map[string]interface{}) (int, int, error)
}