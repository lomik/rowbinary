package rowbinary

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRedirects(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		var mu sync.Mutex
		var finalMethod, finalBody string
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				_, _ = io.Copy(io.Discard, r.Body)
				http.Redirect(w, r, "/next?"+r.URL.RawQuery, code)
				return
			}
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			finalMethod, finalBody = r.Method, string(b)
			mu.Unlock()
		}))
		c := NewClient(WithDSN(s.URL))
		replayable := code == 307 || code == 308
		for name, call := range map[string]func() error{
			"Exec":   func() error { return c.Exec(t.Context(), "SELECT 1") },
			"Select": func() error { return c.Select(t.Context(), "SELECT 1") },
			"Insert": func() error {
				return c.Insert(t.Context(), "t", WithBodyWriter(func(w io.Writer) error { _, err := w.Write([]byte("data")); return err }))
			},
		} {
			mu.Lock()
			finalMethod, finalBody = "", ""
			mu.Unlock()
			err := call()
			mu.Lock()
			method, body := finalMethod, finalBody
			mu.Unlock()
			switch {
			case replayable && name != "Insert":
				// Same method and body at the new location.
				if err != nil || method != http.MethodPost || body != "SELECT 1" {
					t.Errorf("%d %s: err %v, target got %s %q", code, name, err, method, body)
				}
			case replayable:
				// A streamed body cannot be re-sent: a clear error, no request.
				if err == nil || !strings.Contains(err.Error(), "not followed") || method != "" {
					t.Errorf("%d %s: err %v, target got %s", code, name, err, method)
				}
			default:
				// The client re-sends as GET without the query: never a success.
				if err == nil || !strings.Contains(err.Error(), "redirected") {
					t.Errorf("%d %s: err %v (target got %s %q)", code, name, err, method, body)
				}
			}
		}
		s.Close()
	}
}

func TestTLS(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer s.Close()
	if err := NewClient(WithDSN(s.URL), WithHTTPClient(s.Client())).Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("trusted TLS: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := NewClient(WithDSN(s.URL)).Exec(ctx, "SELECT 1")
	if err == nil || errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("untrusted TLS: err %v, want a certificate error", err)
	}
}

func TestDiscovery(t *testing.T) {
	s := drainServer(t)
	failing := WithDiscovery(func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error) {
		return "", errors.New("no hosts")
	})
	requests := 0
	counting := WithHTTPClient(&http.Client{Transport: countingTransport{&requests}})
	c := NewClient(failing, counting)
	writer := WithBodyWriter(func(w io.Writer) error { return nil })
	for name, err := range map[string]error{
		"Exec":   c.Exec(t.Context(), "SELECT 1"),
		"Select": c.Select(t.Context(), "SELECT 1"),
		"Insert": c.Insert(t.Context(), "t", writer),
	} {
		if err == nil || !strings.Contains(err.Error(), "no hosts") {
			t.Errorf("%s: err %v, want the discovery error", name, err)
		}
	}
	if requests != 0 {
		t.Errorf("%d requests sent despite discovery errors", requests)
	}

	// The discovered DSN is used, and each call reports its method.
	var methods []ClientMethod
	c = NewClient(WithDSN("http://unused.invalid"), WithDiscovery(func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error) {
		methods = append(methods, kind.Method)
		return s.URL, nil
	}))
	if err := c.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Select(t.Context(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Insert(t.Context(), "t", writer); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 3 || methods[0] != ClientMethodExecute || methods[1] != ClientMethodSelect || methods[2] != ClientMethodInsert {
		t.Errorf("methods = %v", methods)
	}

	// A client's own discovery overrides the global one.
	SetGlobalDiscovery(func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error) {
		return "", errors.New("global used")
	})
	defer SetGlobalDiscovery(nil)
	if err := NewClient(WithDSN(s.URL)).Exec(t.Context(), "SELECT 1"); err == nil || !strings.Contains(err.Error(), "global used") {
		t.Errorf("global discovery: err %v", err)
	}
	own := WithDiscovery(func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error) { return s.URL, nil })
	if err := NewClient(own).Exec(t.Context(), "SELECT 1"); err != nil {
		t.Errorf("client discovery over global: %v", err)
	}
}

func TestConnectionReuse(t *testing.T) {
	s, conns := countingServer(t) // from client_close_test.go
	c := NewClient(WithDSN(s.URL))
	for range 3 {
		if err := c.Exec(t.Context(), "SELECT 1"); err != nil {
			t.Fatal(err)
		}
		if err := c.Insert(t.Context(), "t", WithBodyWriter(func(w io.Writer) error { _, err := w.Write([]byte("x")); return err })); err != nil {
			t.Fatal(err)
		}
		if err := c.Select(t.Context(), "SELECT 1"); err != nil {
			t.Fatal(err)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Errorf("%d connections for 9 sequential requests, want 1", n)
	}
}

func TestInsertEarlyErrorResponse(t *testing.T) {
	// The server rejects the insert without reading the body.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Code: 60. DB::Exception: Unknown table", http.StatusNotFound)
	}))
	defer s.Close()
	chunk := bytes.Repeat([]byte("x"), 1<<20)
	done := make(chan error, 1)
	go func() {
		done <- NewClient(WithDSN(s.URL)).Insert(t.Context(), "missing", WithBodyWriter(func(w io.Writer) error {
			for range 1024 { // 1 GiB if nothing stops it
				if _, err := w.Write(chunk); err != nil {
					return err
				}
			}
			return nil
		}))
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "Unknown table") {
			t.Fatalf("err = %v, want the server's error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Insert did not return after the early error response")
	}
}

func TestFormatReaderTakesPrecedenceOverBodyReader(t *testing.T) {
	s := drainServer(t)
	var used []string
	err := NewClient(WithDSN(s.URL)).Select(t.Context(), "SELECT 1",
		WithBodyReader(func(r io.Reader) error { used = append(used, "body"); return nil }),
		WithFormatReader(func(r *FormatReader) error { used = append(used, "format"); return nil }))
	if err != nil || len(used) != 1 || used[0] != "format" {
		t.Fatalf("used %v, err %v", used, err)
	}
}
