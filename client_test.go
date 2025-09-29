package rowbinary

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

var testClickHouseDSN = "http://127.0.0.1:8123"

var testTableCounter atomic.Uint64

type testCase struct {
	db            string
	defaultClient Client
}

func newTestCase() *testCase {
	db := fmt.Sprintf("db_%d_%d", testTableCounter.Add(1), time.Now().UnixNano())
	defaultClient := NewClient(context.Background(), testClickHouseDSN, nil)

	err := defaultClient.Exec(context.Background(), "CREATE DATABASE "+db)
	if err != nil {
		log.Fatal(err)
	}

	return &testCase{
		db:            db,
		defaultClient: defaultClient,
	}
}

func (tc *testCase) Database() string {
	return tc.db
}

func (tc *testCase) Close() {
	err := tc.defaultClient.Exec(context.Background(), "DROP DATABASE "+tc.db)
	if err != nil {
		log.Fatal(err)
	}
}
