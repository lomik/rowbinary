package rowbinary

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewTestClientEHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := NewTestClientE(ctx, "http://127.0.0.1:1"); err == nil {
		t.Fatal("expected canceled setup error")
	}
}

func TestTestClientDatabaseLifecycle(t *testing.T) {
	var queries []string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		queries = append(queries, string(b))
	}))
	defer s.Close()
	c, err := NewTestClientE(t.Context(), s.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 2 || queries[0] != "CREATE DATABASE "+c.Database() || queries[1] != "DROP DATABASE "+c.Database() {
		t.Fatalf("queries: %v", queries)
	}
}
