package rowbinary

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestExternalDataMissingWriter(t *testing.T) {
	if os.Getenv("ROWBINARY_MISSING_WRITER_CHILD") == "1" {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
		}))
		defer s.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		err := NewClient(WithDSN(s.URL)).Select(ctx, "SELECT * FROM data", WithExternalData("data", C("x", UInt8)))
		if err == nil {
			t.Fatal("expected missing writer error")
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestExternalDataMissingWriter$", "-test.timeout=4s")
	cmd.Env = append(os.Environ(), "ROWBINARY_MISSING_WRITER_CHILD=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("missing writer did not return a normal error: %v\n%s", err, out)
	}
}

func TestConcurrentExternalDataOptionReuse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	defer s.Close()
	c := NewClient(WithDSN(s.URL))
	ed := WithExternalData("data", C("a", UInt8), C("b", UInt8), C("c", UInt8), WithFormatWriter(func(w *FormatWriter) error { return nil }))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 10; j++ {
				if err := c.Select(ctx, "SELECT * FROM data", ed); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}
