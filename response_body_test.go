package rowbinary

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

func TestErrorBodyIsCapped(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Code: 999. DB::Exception: "))
		w.Write(bytes.Repeat([]byte("x"), 10<<20))
	}))
	defer s.Close()
	c := NewClient(WithDSN(s.URL))
	body := WithBodyWriter(func(w io.Writer) error { return nil })

	for name, err := range map[string]error{
		"Exec":   c.Exec(t.Context(), "SELECT 1"),
		"Select": c.Select(t.Context(), "SELECT 1"),
		"Insert": c.Insert(t.Context(), "t", body),
	} {
		if err == nil {
			t.Errorf("%s: nil error for HTTP 500", name)
			continue
		}
		msg := err.Error()
		if len(msg) > maxErrorBody+200 || !strings.Contains(msg, "truncated") || !strings.Contains(msg, "500") || !strings.Contains(msg, "Code: 999") {
			t.Errorf("%s: error of %d bytes, want status, start of body and a truncation note", name, len(msg))
		}
	}
}

func TestSmallErrorBodyIsQuotedWhole(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Code: 62. DB::Exception: Syntax error"))
	}))
	defer s.Close()
	err := NewClient(WithDSN(s.URL)).Exec(t.Context(), "SELEC")
	if err == nil || err.Error() != "unexpected status code: 400 (Code: 62. DB::Exception: Syntax error)" {
		t.Fatalf("err = %v", err)
	}
}

func TestSuccessBodyIsNotBuffered(t *testing.T) {
	const size = 64 << 20
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		chunk := make([]byte, 1<<20)
		for range size / len(chunk) {
			w.Write(chunk)
		}
	}))
	defer s.Close()
	c := NewClient(WithDSN(s.URL))
	body := WithBodyWriter(func(w io.Writer) error { return nil })

	for name, call := range map[string]func() error{
		"Exec":   func() error { return c.Exec(t.Context(), "SELECT 1") },
		"Insert": func() error { return c.Insert(t.Context(), "t", body) },
	} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		if err := call(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		runtime.ReadMemStats(&after)
		// The client's 1 MiB transport buffers and the test server's own
		// allocations stay well below the 64 MiB body.
		if grew := after.TotalAlloc - before.TotalAlloc; grew > 24<<20 {
			t.Errorf("%s allocated %d MiB for a %d MiB response", name, grew>>20, size>>20)
		}
	}
}
