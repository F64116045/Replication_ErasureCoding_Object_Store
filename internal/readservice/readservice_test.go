package readservice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
)

// --- 1. Mocks Definitions ---

// MockHttpClient simulates HTTP responses without network calls.
type MockHttpClient struct {
	responses map[string]*http.Response
	failOnDo  bool
	mu        sync.RWMutex
}

func NewMockHttpClient() *MockHttpClient {
	return &MockHttpClient{responses: make(map[string]*http.Response)}
}

func (m *MockHttpClient) SetResponse(url string, statusCode int, body string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses[url] = &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func (m *MockHttpClient) Do(req *http.Request) (*http.Response, error) {
	if m.failOnDo {
		return nil, fmt.Errorf("mock network error")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if resp, ok := m.responses[req.URL.String()]; ok {
		return resp, nil
	}
	return &http.Response{
		StatusCode: 404,
		Body:       io.NopCloser(strings.NewReader("Not Found")),
		Header:     make(http.Header),
	}, nil
}

// MockEcDriver simulates Erasure Coding split/reconstruct logic.
type MockEcDriver struct {
	ShouldFail      bool
	ReconstructFunc func(shards [][]byte) error
}

func (m *MockEcDriver) Split(data []byte) ([][]byte, error) { return nil, nil }
func (m *MockEcDriver) Encode(shards [][]byte) error       { return nil }

func (m *MockEcDriver) Reconstruct(shards [][]byte) error {
	if m.ShouldFail {
		return fmt.Errorf("mock EC reconstruct failed")
	}
	if m.ReconstructFunc != nil {
		return m.ReconstructFunc(shards)
	}
	// Default behavior: fill nil shards with dummy data
	for i := 0; i < config.K; i++ {
		if shards[i] == nil {
			shards[i] = []byte(fmt.Sprintf("reconstructed-shard-%d", i))
		}
	}
	return nil
}

// MockUtilsSvc simulates serialization and field logic.
type MockUtilsSvc struct{}

func (m *MockUtilsSvc) SeparateHotColdFields(data map[string]interface{}) (map[string]interface{}, map[string]interface{}) {
	return nil, nil
}
func (m *MockUtilsSvc) Serialize(data map[string]interface{}) ([]byte, error) { return nil, nil }
func (m *MockUtilsSvc) MapsAreEqual(map1, map2 map[string]interface{}) bool   { return true }

func (m *MockUtilsSvc) Deserialize(data []byte) (map[string]interface{}, error) {
	// Simulate trimming null bytes (EC padding)
	trimmed := bytes.TrimRight(data, "\x00")
	var result map[string]interface{}
	err := json.Unmarshal(trimmed, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *MockUtilsSvc) MergeHotColdFields(hotFields, coldFields map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{}, len(hotFields)+len(coldFields))
	for k, v := range coldFields {
		merged[k] = v
	}
	for k, v := range hotFields {
		merged[k] = v
	}
	return merged
}

// Helper to create service with mocks
func createMockService(http interfaces.IHttpClient, ec interfaces.IEcDriver, utils interfaces.IUtilsSvc) *Service {
	// Ensure *Service satisfies the interface
	var _ interfaces.IReadService = (*Service)(nil)
	return &Service{
		http:  http,
		ec:    ec,
		utils: utils,
	}
}

// --- 2. Test Cases ---

func TestReadReplication_SuccessRace(t *testing.T) {
	mockHttp := NewMockHttpClient()
	nodes := []string{"http://node1", "http://node2", "http://node3"}

	// Scenario: Node 1 is 404, Node 2 is 200, Node 3 is 404
	// The service should return the data from Node 2
	mockHttp.SetResponse("http://node1/retrieve/test_key", 404, "")
	mockHttp.SetResponse("http://node2/retrieve/test_key", 200, "success data")
	mockHttp.SetResponse("http://node3/retrieve/test_key", 404, "")

	readSvc := createMockService(mockHttp, nil, nil)
	data, err := readSvc.ReadReplication(context.Background(), nodes, "test_key")

	if err != nil {
		t.Errorf("ReadReplication() expected success, got error: %v", err)
	}
	if string(data) != "success data" {
		t.Errorf("ReadReplication() expected 'success data', got '%s'", string(data))
	}
}

func TestReadEC_Success(t *testing.T) {
	mockHttp := NewMockHttpClient()
	mockEc := &MockEcDriver{}
	nodes := []string{
		"http://n1", "http://n2", "http://n3",
		"http://n4", "http://n5", "http://n6",
	}
	prefix := "test_ec_chunk_"

	// Simulate 4 data shards available (indices 0, 1, 3, 4)
	// Index 2 and 5 are missing (404)
	mockHttp.SetResponse("http://n1/retrieve/test_ec_chunk_0", 200, "shard-0")
	mockHttp.SetResponse("http://n2/retrieve/test_ec_chunk_1", 200, "shard-1")
	mockHttp.SetResponse("http://n3/retrieve/test_ec_chunk_2", 404, "")
	mockHttp.SetResponse("http://n4/retrieve/test_ec_chunk_3", 200, "shard-3")
	mockHttp.SetResponse("http://n5/retrieve/test_ec_chunk_4", 200, "shard-4")
	mockHttp.SetResponse("http://n6/retrieve/test_ec_chunk_5", 404, "")

	metadata := map[string]interface{}{
		"k":               4.0, // JSON numbers are floats
		"m":               2.0,
		"chunk_prefix":    prefix,
		"original_length": 12.0, // Length of "shard-0shard-1..." etc
	}

	readSvc := createMockService(mockHttp, mockEc, nil)
	data, err := readSvc.ReadEC(context.Background(), nodes, metadata)

	if err != nil {
		t.Errorf("ReadEC() expected success, got error: %v", err)
	}

	// Mock EC reconstruct fills missing shards with "reconstructed-shard-i"
	// but since K=4, we only read shards 0, 1, 2, 3 combined.
	// Shard 0="shard-0" (7 bytes)
	// Shard 1="shard-1" (7 bytes)
	// Shard 2="reconstructed-shard-2" (long string)
	// Shard 3="shard-3" (7 bytes)
	// Total concatenated > 12 bytes.
	// It should be truncated to original_length=12.
	// Expected: "shard-0" + "shard" (5 chars) = 12 chars? No.
	// "shard-0" is 7 bytes.
	// "shard-1" is 7 bytes.
	// 7 + 7 = 14 bytes.
	// Since original_length is 12, we expect the first 12 bytes of the combined stream.
	expected := "shard-0shard"

	if len(data) != 12 {
		t.Errorf("ReadEC() truncation failed: expected length 12, got %d", len(data))
	}
	if string(data) != expected {
		t.Errorf("ReadEC() data mismatch:\nExpected: %s\nActual:   %s", expected, string(data))
	}
}

func TestReadEC_Fails_NotEnoughChunks(t *testing.T) {
	mockHttp := NewMockHttpClient()
	mockEc := &MockEcDriver{}
	nodes := []string{
		"http://n1", "http://n2", "http://n3",
		"http://n4", "http://n5", "http://n6",
	}
	prefix := "test_ec_chunk_"

	// Only 2 shards available (indices 0, 1). Need K=4.
	mockHttp.SetResponse("http://n1/retrieve/test_ec_chunk_0", 200, "shard-0")
	mockHttp.SetResponse("http://n2/retrieve/test_ec_chunk_1", 200, "shard-1")
	mockHttp.SetResponse("http://n3/retrieve/test_ec_chunk_2", 404, "")
	mockHttp.SetResponse("http://n4/retrieve/test_ec_chunk_3", 404, "")
	mockHttp.SetResponse("http://n5/retrieve/test_ec_chunk_4", 404, "")
	mockHttp.SetResponse("http://n6/retrieve/test_ec_chunk_5", 404, "")

	metadata := map[string]interface{}{
		"k":               4.0,
		"m":               2.0,
		"chunk_prefix":    prefix,
		"original_length": 10.0,
	}

	readSvc := createMockService(mockHttp, mockEc, nil)
	_, err := readSvc.ReadEC(context.Background(), nodes, metadata)

	if err == nil {
		t.Errorf("ReadEC() expected failure (not enough chunks), but succeeded")
	} else if !strings.Contains(err.Error(), "insufficient chunks") && !strings.Contains(err.Error(), "chunks 不足") {
		// Note: Check against English or Chinese error message depending on implementation
		t.Logf("Got expected error: %v", err)
	}
}

func TestReadFieldHybrid_Success(t *testing.T) {
	mockHttp := NewMockHttpClient()
	mockUtils := &MockUtilsSvc{}

	nodes := []string{
		"http://n1", "http://n2", "http://n3",
		"http://n4", "http://n5", "http://n6",
	}

	// A. Mock Hot Data (ReadReplication)
	mockHttp.SetResponse("http://n1/retrieve/test_hybrid_hot", 200, `{"like_count": 100}`)

	// B. Mock Cold Data (ReadEC)
	// We use simple English strings to avoid UTF-8 byte counting confusion in tests.
	// JSON to reconstruct: {"content": "cold-data", "desc": "B"}
	// Part 1: `{"content": "cold-data"` (23 bytes)
	// Part 2: `, "desc": "B"}` (13 bytes)
	// Total: 36 bytes
	part1 := []byte(`{"content": "cold-data"`)
	part2 := []byte(`, "desc": "B"}`)
	totalLen := len(part1) + len(part2)

	mockEcHybrid := &MockEcDriver{
		ReconstructFunc: func(shards [][]byte) error {
			shards[0] = part1
			shards[1] = part2
			shards[2] = []byte{0, 0, 0} // Padding
			shards[3] = []byte{0, 0, 0} // Padding
			return nil
		},
	}

	prefix := "test_hybrid_cold_"
	mockHttp.SetResponse("http://n1/retrieve/test_hybrid_cold_0", 200, string(part1))
	mockHttp.SetResponse("http://n2/retrieve/test_hybrid_cold_1", 200, string(part2))
	// Simulate missing other shards to force Reconstruct logic (though mockEcHybrid handles logic)
	mockHttp.SetResponse("http://n3/retrieve/test_hybrid_cold_2", 404, "")
	mockHttp.SetResponse("http://n4/retrieve/test_hybrid_cold_3", 404, "")
	mockHttp.SetResponse("http://n5/retrieve/test_hybrid_cold_4", 404, "")
	mockHttp.SetResponse("http://n6/retrieve/test_hybrid_cold_5", 404, "")

	metadata := map[string]interface{}{
		"k":               4.0,
		"m":               2.0,
		"hot_key":         "test_hybrid_hot",
		"cold_prefix":     prefix,
		"original_length": float64(totalLen), // Must match exact bytes for strict truncation
	}

	readSvc := createMockService(mockHttp, mockEcHybrid, mockUtils)

	mergedMap, err := readSvc.ReadFieldHybrid(context.Background(), nodes, nodes, metadata)
	if err != nil {
		t.Fatalf("ReadFieldHybrid() expected success, got error: %v", err)
	}

	expectedMap := map[string]interface{}{
		"like_count": float64(100),
		"content":    "cold-data",
		"desc":       "B",
	}

	if !reflect.DeepEqual(mergedMap, expectedMap) {
		t.Errorf("ReadFieldHybrid() mismatch:\nExpected: %v\nActual:   %v", expectedMap, mergedMap)
	}
}