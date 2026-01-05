package s3

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/magiconair/properties"
	gofakes3 "go.sia.tech/gofakes3"
	"go.sia.tech/gofakes3/backend/s3mem"
)

func newTestDB(t *testing.T) (*s3DB, func()) {
	backend := s3mem.New()
	fake, err := gofakes3.New(backend)
	if err != nil {
		t.Fatalf("failed to create fake s3: %v", err)
	}

	srv := httptest.NewServer(fake.Server())

	p := properties.NewProperties()
	p.Set(s3Bucket, "ycsb")
	p.Set(s3Endpoint, srv.URL)
	p.Set(s3Region, "us-east-1")
	p.Set(s3AccessKey, "dummy")
	p.Set(s3SecretKey, "dummy")
	p.Set(s3UsePathStyle, "true")

	dbi, err := s3Creator{}.Create(p)
	if err != nil {
		t.Fatalf("create db: %v", err)
	}
	db := dbi.(*s3DB)

	cleanup := func() {
		db.Close()
		srv.Close()
	}
	return db, cleanup
}

func TestInsertReadDelete(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	key := "k1"
	vals := map[string][]byte{"k": []byte("v")}

	if err := db.Insert(ctx, table, key, vals); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := db.Read(ctx, table, key, nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !reflect.DeepEqual(got, vals) {
		t.Fatalf("read mismatch, got %v want %v", got, vals)
	}

	if err := db.Delete(ctx, table, key); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestScan(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	for i := 0; i < 3; i++ {
		key := "key" + string(rune('A'+i))
		val := map[string][]byte{"k": {byte(i)}}

		if err := db.Insert(ctx, table, key, val); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	res, err := db.Scan(ctx, table, "keyA", 3, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 results, got %d", len(res))
	}
}

func TestScanKeysOnly(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	for i := 0; i < 3; i++ {
		key := "key" + string(rune('A'+i))
		val := map[string][]byte{"k": {byte(i)}}

		if err := db.Insert(ctx, table, key, val); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	db.scanKeysOnly = true
	res, err := db.Scan(ctx, table, "keyA", 3, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 results, got %d", len(res))
	}
	for i := 0; i < 3; i++ {
		key := "key" + string(rune('A'+i))
		if _, ok := res[i][key]; !ok {
			t.Fatalf("expected key %s in result, got %v", key, res[i])
		}
	}
}

func TestUpdate(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	key := "k1"
	vals := map[string][]byte{"k": []byte("v")}

	if err := db.Insert(ctx, table, key, vals); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// update with overwrite
	if err := db.Update(ctx, table, key, map[string][]byte{"k2": []byte("v2")}); err != nil {
		t.Fatalf("update: %v", err)
	}

	// read the updated value
	got, err := db.Read(ctx, table, key, nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !reflect.DeepEqual(got, map[string][]byte{"k2": []byte("v2")}) {
		t.Fatalf("read mismatch, got %v want %v", got, map[string][]byte{"k2": []byte("v2")})
	}
}

func TestUpdateNoOverwrite(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	key := "k1"
	vals := map[string][]byte{"k": []byte("v")}

	if err := db.Insert(ctx, table, key, vals); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// update without overwrite
	db.updateOverwrite = false
	if err := db.Update(ctx, table, key, map[string][]byte{"k2": []byte("v2")}); err != nil {
		t.Fatalf("update: %v", err)
	}

	// read the original value
	got, err := db.Read(ctx, table, key, nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !reflect.DeepEqual(got, map[string][]byte{"k": []byte("v"), "k2": []byte("v2")}) {
		t.Fatalf("read mismatch, got %v want %v", got, map[string][]byte{"k": []byte("v"), "k2": []byte("v2")})
	}
}
