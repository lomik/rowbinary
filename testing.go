package rowbinary

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync/atomic"
	"time"
)

// requests clickhouse, caching locally to disk
// re-running the test can already work without CH. including in CI if you commit fixtures/*
func ExecLocal(query string) ([]byte, error) {
	h := sha256.New()
	h.Write([]byte(query))
	key := fmt.Sprintf("%x", h.Sum(nil))
	filename := fmt.Sprintf("fixtures/ch_%s.bin", key)

	// fmt.Println(filename, query)

	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		body, err := exec.Command("clickhouse", "local", "--query", query).Output()
		if err != nil {
			return nil, err
		}

		err = os.WriteFile(filename, body, 0600)
		return body, err
	}
	// #nosec G304
	return os.ReadFile(filename)
}

var testClientCounter atomic.Uint64

type testClient struct {
	Client
	db string
}

func NewTestClient(ctx context.Context, dsn string, options ...ClientOption) Client {
	db := fmt.Sprintf("db_%d_%d", testClientCounter.Add(1), time.Now().UnixNano())
	defaultClient := NewClient(context.Background(), dsn, append(options, WithDatabase("default"))...)

	err := defaultClient.Exec(context.Background(), "CREATE DATABASE "+db)
	if err != nil {
		log.Fatal(err)
	}
	defaultClient.Close()

	return &testClient{
		Client: NewClient(ctx, dsn, append(options, WithDatabase(db))...),
		db:     db,
	}
}

func (tc *testClient) Close() error {
	return tc.Exec(context.Background(), "DROP DATABASE "+tc.db)
}
