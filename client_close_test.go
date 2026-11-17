package rowbinary

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// countingServer counts the TCP connections clients open to it.
func countingServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	var conns atomic.Int32
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	s.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			conns.Add(1)
		}
	}
	s.Start()
	t.Cleanup(s.Close)
	return s, &conns
}

func TestCloseLeavesSharedPoolOpen(t *testing.T) {
	s, conns := countingServer(t)
	a, b := NewClient(WithDSN(s.URL)), NewClient(WithDSN(s.URL))
	if err := a.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	// b reuses the pooled connection; a remains usable.
	if err := b.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("closed client unusable: %v", err)
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("server saw %d connections, want 1 (Close must not drop pooled connections)", n)
	}
}

func TestCloseLeavesCallerHTTPClientOpen(t *testing.T) {
	s, conns := countingServer(t)
	own := &http.Client{Transport: &http.Transport{}}
	c := NewClient(WithDSN(s.URL), WithHTTPClient(own))
	if err := c.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// The caller's other users of the same http.Client keep their connection.
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, s.URL, nil)
	resp, err := own.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if n := conns.Load(); n != 1 {
		t.Fatalf("server saw %d connections, want 1 (Close must not touch the caller's http.Client)", n)
	}
}
