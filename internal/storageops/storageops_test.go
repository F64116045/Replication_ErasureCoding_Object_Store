package storageops

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"hybrid_distributed_store/internal/interfaces"
)

// --- 1. Mocks Definitions ---

// MockHttpClient simulates an HTTP client that records called URLs.
type MockHttpClient struct {
	CalledURLs map[string]int
	mu         sync.RWMutex
}

// NewMockHttpClient creates a new instance of the mock client.
func NewMockHttpClient() *MockHttpClient {
	return &MockHttpClient{
		CalledURLs: make(map[string]int),
	}
}

// Do implements the IHttpClient interface.
// It records the URL and returns a 200 OK response.
func (m *MockHttpClient) Do(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	m.CalledURLs[req.URL.String()]++
	m.mu.Unlock()

	// Simulate success
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
	}, nil
}

// createMockService helper to inject the mock client into the service.
func createMockService(http interfaces.IHttpClient) *Service {
	// Ensure *Service satisfies the interface
	var _ interfaces.IStorageOps = (*Service)(nil)

	return &Service{
		http: http,
	}
}

// --- 2. Test Cases ---

func TestDeleteReplication(t *testing.T) {
	// --- 1. Arrange ---
	mockHttp := NewMockHttpClient()
	svc := createMockService(mockHttp)

	nodes := []string{"http://n1", "http://n2", "http://n3"}
	key := "test_rep_key"
	ctx := context.Background()

	// --- 2. Act ---
	count, err := svc.DeleteReplication(ctx, nodes, key)

	// --- 3. Assert ---
	if err != nil {
		t.Fatalf("DeleteReplication() expected success, got error: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected successCount 3, got %d", count)
	}

	expectedURLs := map[string]int{
		"http://n1/delete/test_rep_key": 1,
		"http://n2/delete/test_rep_key": 1,
		"http://n3/delete/test_rep_key": 1,
	}
	if !reflect.DeepEqual(mockHttp.CalledURLs, expectedURLs) {
		t.Errorf("URL call mismatch:\nExpected: %v\nActual:   %v", expectedURLs, mockHttp.CalledURLs)
	}
}

func TestDeleteEC(t *testing.T) {
	// --- 1. Arrange ---
	mockHttp := NewMockHttpClient()
	svc := createMockService(mockHttp)

	nodes := []string{"http://n1", "http://n2", "http://n3", "http://n4", "http://n5", "http://n6"}
	metadata := map[string]interface{}{
		"key_name":     "test_ec_key",
		"chunk_prefix": "my_ec_prefix_",
	}
	ctx := context.Background()

	// --- 2. Act ---
	count, err := svc.DeleteEC(ctx, nodes, metadata)

	// --- 3. Assert ---
	if err != nil {
		t.Fatalf("DeleteEC() expected success, got error: %v", err)
	}
	if count != 6 {
		t.Errorf("Expected successCount 6, got %d", count)
	}

	expectedURLs := map[string]int{
		"http://n1/delete/my_ec_prefix_0": 1,
		"http://n2/delete/my_ec_prefix_1": 1,
		"http://n3/delete/my_ec_prefix_2": 1,
		"http://n4/delete/my_ec_prefix_3": 1,
		"http://n5/delete/my_ec_prefix_4": 1,
		"http://n6/delete/my_ec_prefix_5": 1,
	}
	if !reflect.DeepEqual(mockHttp.CalledURLs, expectedURLs) {
		t.Errorf("URL call mismatch:\nExpected: %v\nActual:   %v", expectedURLs, mockHttp.CalledURLs)
	}
}

func TestDeleteFieldHybrid(t *testing.T) {
	// --- 1. Arrange ---
	mockHttp := NewMockHttpClient()
	svc := createMockService(mockHttp)

	replicaNodes := []string{"http://n1", "http://n2", "http://n3"}
	ecNodes := []string{"http://n1", "http://n2", "http://n3", "http://n4", "http://n5", "http://n6"}

	metadata := map[string]interface{}{
		"key_name":    "test_hybrid_key",
		"hot_key":     "test_hybrid_hot",
		"cold_prefix": "test_hybrid_cold_chunk_",
	}
	ctx := context.Background()

	// --- 2. Act ---
	hotCount, coldCount, err := svc.DeleteFieldHybrid(ctx, replicaNodes, ecNodes, metadata)

	// --- 3. Assert ---
	if err != nil {
		t.Fatalf("DeleteFieldHybrid() expected success, got error: %v", err)
	}
	if hotCount != 3 {
		t.Errorf("Expected hotSuccess 3, got %d", hotCount)
	}
	if coldCount != 6 {
		t.Errorf("Expected coldSuccess 6, got %d", coldCount)
	}

	expectedURLs := map[string]int{
		// Hot deletion calls
		"http://n1/delete/test_hybrid_hot": 1,
		"http://n2/delete/test_hybrid_hot": 1,
		"http://n3/delete/test_hybrid_hot": 1,
		// Cold deletion calls
		"http://n1/delete/test_hybrid_cold_chunk_0": 1,
		"http://n2/delete/test_hybrid_cold_chunk_1": 1,
		"http://n3/delete/test_hybrid_cold_chunk_2": 1,
		"http://n4/delete/test_hybrid_cold_chunk_3": 1,
		"http://n5/delete/test_hybrid_cold_chunk_4": 1,
		"http://n6/delete/test_hybrid_cold_chunk_5": 1,
	}
	if !reflect.DeepEqual(mockHttp.CalledURLs, expectedURLs) {
		t.Errorf("URL call mismatch:\nExpected: %v\nActual:   %v", expectedURLs, mockHttp.CalledURLs)
	}
}

func TestDeleteFieldHybrid_BlindDelete(t *testing.T) {
	// --- 1. Arrange ---
	mockHttp := NewMockHttpClient()
	svc := createMockService(mockHttp)

	replicaNodes := []string{"http://n1", "http://n2", "http://n3"}
	ecNodes := []string{"http://n1", "http://n2", "http://n3", "http://n4", "http://n5", "http://n6"}

	// Blind delete: metadata only contains key_name
	metadata := map[string]interface{}{
		"key_name": "test_blind_key",
	}
	ctx := context.Background()

	// --- 2. Act ---
	_, _, err := svc.DeleteFieldHybrid(ctx, replicaNodes, ecNodes, metadata)

	// --- 3. Assert ---
	if err != nil {
		t.Fatalf("DeleteFieldHybrid() (Blind Delete) expected success, got error: %v", err)
	}

	// Verify that the service correctly guessed the hot key and cold prefix
	expectedURLs := map[string]int{
		// Guess: key + "_hot"
		"http://n1/delete/test_blind_key_hot": 1,
		"http://n2/delete/test_blind_key_hot": 1,
		"http://n3/delete/test_blind_key_hot": 1,
		// Guess: key + "_cold_chunk_"
		"http://n1/delete/test_blind_key_cold_chunk_0": 1,
		"http://n2/delete/test_blind_key_cold_chunk_1": 1,
		"http://n3/delete/test_blind_key_cold_chunk_2": 1,
		"http://n4/delete/test_blind_key_cold_chunk_3": 1,
		"http://n5/delete/test_blind_key_cold_chunk_4": 1,
		"http://n6/delete/test_blind_key_cold_chunk_5": 1,
	}
	if !reflect.DeepEqual(mockHttp.CalledURLs, expectedURLs) {
		t.Errorf("URL call mismatch:\nExpected: %v\nActual:   %v", expectedURLs, mockHttp.CalledURLs)
	}
}