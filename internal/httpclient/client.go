package httpclient

import (
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	// _client is the Singleton instance
	_client *http.Client
	// once ensures initialization runs only once
	once sync.Once
)

// GetClient returns the singleton HTTP client instance.
func GetClient() *http.Client {
	once.Do(func() {
		// Configure connection pooling for high concurrency
		transport := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,  // Connection timeout
				KeepAlive: 30 * time.Second, // TCP KeepAlive
			}).DialContext,
			MaxIdleConns:        100,              // Total max idle connections
			MaxIdleConnsPerHost: 20,               // Max idle connections per host (e.g., node_1)
			IdleConnTimeout:     90 * time.Second, // How long to keep idle connections alive
		}

		_client = &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second, // Total timeout per request
		}
	})
	return _client
}

