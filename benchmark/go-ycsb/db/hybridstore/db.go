package hybridstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"


	"github.com/magiconair/properties"
	
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type hybridDB struct {
	endpoint string
	client   *http.Client
	strategy string
}

func (db *hybridDB) Close() error { return nil }


func (db *hybridDB) InitThread(ctx context.Context, _ int, _ int) context.Context { 
	return ctx 
}

func (db *hybridDB) CleanupThread(_ context.Context) {}

func (db *hybridDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

func (db *hybridDB) Delete(ctx context.Context, table string, key string) error {
	url := fmt.Sprintf("%s/delete/%s", db.endpoint, key)
	req, _ := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	resp, err := db.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (db *hybridDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	data := make(map[string]interface{})
	for k, v := range values {
		data[k] = string(v)
	}

	data["like_count"] = 0
	data["view_count"] = 0

	rawLog := make([]byte, 100 * 1024)
	for i := range rawLog {
		rawLog[i] = 'A'
	}
	data["sensor_raw_log"] = string(rawLog)

	return db.sendRequest(ctx, key, data)
}

func (db *hybridDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	data := make(map[string]interface{})

	data["view_count"] = time.Now().Unix()

	return db.sendRequest(ctx, key, data)
}

func (db *hybridDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	url := fmt.Sprintf("%s/read/%s", db.endpoint, key)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := db.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("read failed with status: %d", resp.StatusCode)
	}
	return nil, nil
}

func (db *hybridDB) sendRequest(ctx context.Context, key string, data interface{}) error {
	body, _ := json.Marshal(data)
	url := fmt.Sprintf("%s/write?key=%s&strategy=%s", db.endpoint, key, db.strategy)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := db.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API error: %d", resp.StatusCode)
	}
	return nil
}

type hybridDBCreator struct{}

func (c hybridDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	endpoint := p.GetString("hybridstore.endpoint", "http://localhost:8000")
	strategy := p.GetString("hybridstore.strategy", "field_hybrid")
	
	return &hybridDB{
		endpoint: endpoint,
		strategy: strategy,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
			Timeout: 10 * time.Second,
		},
	}, nil
}

func init() {
	ycsb.RegisterDBCreator("hybridstore", hybridDBCreator{})
}