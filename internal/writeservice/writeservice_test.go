package writeservice

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"hybrid_distributed_store/internal/interfaces"
)

// --- 1. Mocks ---

// MockHttpClient simulates HTTP responses.
type MockHttpClient struct {
	StatusCode int
	Body       string
	ShouldFail bool
}

func (m *MockHttpClient) Do(req *http.Request) (*http.Response, error) {
	if m.ShouldFail {
		return nil, fmt.Errorf("mock network error")
	}
	resp := &http.Response{
		StatusCode: m.StatusCode,
		Body:       io.NopCloser(strings.NewReader(m.Body)),
		Header:     make(http.Header),
	}
	return resp, nil
}

// MockReadService implements IReadService.
type MockReadService struct{}

func (m *MockReadService) CheckFirstWrite(ctx context.Context, replicaNodes []string, hotKey string) (bool, error) {
	return true, nil
}
func (m *MockReadService) GetExistingColdFields(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}
func (m *MockReadService) ReadReplication(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
	return nil, nil
}
func (m *MockReadService) ReadEC(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
	return nil, nil
}
func (m *MockReadService) ReadFieldHybrid(ctx context.Context, replicaNodes, ecNodes []string, metadata map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}

// MockEcDriver implements IEcDriver.
type MockEcDriver struct{}

func (m *MockEcDriver) Split(data []byte) ([][]byte, error) { return nil, nil }
func (m *MockEcDriver) Encode(shards [][]byte) error       { return nil }
func (m *MockEcDriver) Reconstruct(shards [][]byte) error  { return nil }

// MockUtilsSvc implements IUtilsSvc.
type MockUtilsSvc struct{}

func (m *MockUtilsSvc) SeparateHotColdFields(data map[string]interface{}) (map[string]interface{}, map[string]interface{}) {
	return nil, nil
}
func (m *MockUtilsSvc) Serialize(data map[string]interface{}) ([]byte, error) { return nil, nil }
func (m *MockUtilsSvc) Deserialize(data []byte) (map[string]interface{}, error) { return nil, nil }
func (m *MockUtilsSvc) MapsAreEqual(map1, map2 map[string]interface{}) bool { return true }
func (m *MockUtilsSvc) MergeHotColdFields(hotFields, coldFields map[string]interface{}) map[string]interface{} {
	return nil
}

// --- Helper: Start Embedded Etcd ---
// This starts a real, in-memory Etcd server for testing WAL transactions.
func startEmbeddedEtcd(t *testing.T) (*embed.Etcd, *etcd.Client) {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir() // Use temporary directory
	
	// Bind to random ports on localhost
	uClient, _ := url.Parse("http://127.0.0.1:0")
	uPeer, _ := url.Parse("http://127.0.0.1:0")

	cfg.ListenClientUrls = []url.URL{*uClient}
	cfg.ListenPeerUrls = []url.URL{*uPeer}
	cfg.LogLevel = "error" // Reduce noise

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("Failed to start embedded etcd: %v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
		// Server is ready
	case <-time.After(10 * time.Second):
		e.Close()
		t.Fatalf("Embedded etcd took too long to start")
	}

	client, err := etcd.New(etcd.Config{
		Endpoints:   []string{e.Clients[0].Addr().String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to connect to embedded etcd: %v", err)
	}

	return e, client
}

// Helper: Create service with dependencies
func createMockService(etcdClient interfaces.IEtcdClient, httpClient interfaces.IHttpClient) *Service {
	mockRead := &MockReadService{}
	mockEc := &MockEcDriver{}
	mockUtils := &MockUtilsSvc{}

	return NewService(etcdClient, httpClient, mockRead, mockEc, mockUtils)
}

// --- 2. Test Cases ---

func TestWriteReplication_Success(t *testing.T) {
	// 1. Arrange
	etcdServer, etcdClient := startEmbeddedEtcd(t)
	defer etcdServer.Close()
	defer etcdClient.Close()

	mockHttp := &MockHttpClient{
		StatusCode: 200, // Storage nodes return success
		ShouldFail: false,
	}

	writerSvc := createMockService(etcdClient, mockHttp)

	// 2. Act
	nodes := []string{"http://node1"}
	_, err := writerSvc.WriteReplication(context.Background(), nodes, "test_key", []byte("data"))

	// 3. Assert
	if err != nil {
		t.Errorf("WriteReplication() expected success, got error: %v", err)
	}

	// Verify metadata was written to Etcd
	resp, _ := etcdClient.Get(context.Background(), "metadata/test_key")
	if len(resp.Kvs) == 0 {
		t.Errorf("WriteReplication() succeeded, but metadata/test_key was not found in Etcd")
	}
}

func TestWriteReplication_StorageFails(t *testing.T) {
	// 1. Arrange
	etcdServer, etcdClient := startEmbeddedEtcd(t)
	defer etcdServer.Close()
	defer etcdClient.Close()

	mockHttp := &MockHttpClient{
		StatusCode: 500, // Storage nodes return error
		ShouldFail: false,
	}

	writerSvc := createMockService(etcdClient, mockHttp)

	// 2. Act
	nodes := []string{"http://node1"}
	_, err := writerSvc.WriteReplication(context.Background(), nodes, "test_key", []byte("data"))

	// 3. Assert
	// The service should return an error (NOT panic)
	if err == nil {
		t.Errorf("WriteReplication() expected error (storage failed), but got nil")
	}

	// Verify WAL status is FAILED
	resp, _ := etcdClient.Get(context.Background(), "txn/", etcd.WithPrefix())
	foundFailed := false
	for _, kv := range resp.Kvs {
		// Look for any key ending in /status with value "FAILED"
		if strings.HasSuffix(string(kv.Key), "/status") && string(kv.Value) == "FAILED" {
			foundFailed = true
			break
		}
	}

	if !foundFailed {
		t.Errorf("Expected WAL entry with status FAILED, but none found in Etcd")
	}
}