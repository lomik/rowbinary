package rowbinary

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type ctxKey struct{}

func drainServer(t *testing.T) *httptest.Server {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	t.Cleanup(s.Close)
	return s
}

func TestContextOptionsReceiveRequestContext(t *testing.T) {
	s := drainServer(t)
	c := NewClient(WithDSN(s.URL))
	ctx := context.WithValue(t.Context(), ctxKey{}, "request")
	got := map[string]bool{}
	mark := func(name string, ctx context.Context) { got[name] = ctx.Value(ctxKey{}) == "request" }

	if err := c.Insert(ctx, "t", C("x", UInt8), WithFormatWriterContext(func(ctx context.Context, w *FormatWriter) error {
		mark("FormatWriter", ctx)
		return Write(w, UInt8, 1)
	})); err != nil {
		t.Fatal(err)
	}
	if err := c.Insert(ctx, "t", WithBodyWriterContext(func(ctx context.Context, w io.Writer) error {
		mark("BodyWriter", ctx)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := c.Select(ctx, "SELECT", RowBinary, C("x", UInt8), WithFormatReaderContext(func(ctx context.Context, r *FormatReader) error {
		mark("FormatReader", ctx)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := c.Select(ctx, "SELECT", WithBodyReaderContext(func(ctx context.Context, r io.Reader) error {
		mark("BodyReader", ctx)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := c.Select(ctx, "SELECT", WithExternalData("ext", C("x", UInt8), WithFormatWriterContext(func(ctx context.Context, w *FormatWriter) error {
		mark("ExternalData", ctx)
		return Write(w, UInt8, 1)
	}))); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"FormatWriter", "BodyWriter", "FormatReader", "BodyReader", "ExternalData"} {
		if !got[name] {
			t.Errorf("%s callback did not receive the request context", name)
		}
	}
}

func TestBlockedWriterCallbackSeesCancellation(t *testing.T) {
	s := drainServer(t)
	ctx, cancel := context.WithCancel(t.Context())
	started, returned := make(chan struct{}), make(chan struct{})
	insertErr := make(chan error, 1)
	go func() {
		insertErr <- NewClient(WithDSN(s.URL)).Insert(ctx, "t", C("x", UInt8), WithFormatWriterContext(func(ctx context.Context, w *FormatWriter) error {
			close(started)
			<-ctx.Done() // waiting on something other than writes
			close(returned)
			return ctx.Err()
		}))
	}()
	<-started
	cancel()
	select {
	case err := <-insertErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Insert = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Insert did not return after cancellation")
	}
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not see the cancellation")
	}
}

func TestWritesFailAfterCancellation(t *testing.T) {
	s := drainServer(t)
	ctx, cancel := context.WithCancel(t.Context())
	writeErr := make(chan error, 1)
	go func() {
		_ = NewClient(WithDSN(s.URL)).Insert(ctx, "t", C("x", UInt64), WithFormatWriter(func(w *FormatWriter) error {
			for i := 0; ; i++ {
				if i == 1000 {
					cancel()
				}
				if err := Write(w, UInt64, uint64(i)); err != nil {
					writeErr <- err
					return err
				}
			}
		}))
	}()
	select {
	case err := <-writeErr:
		if err == nil {
			t.Fatal("nil write error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writes kept succeeding after cancellation")
	}
}

func TestNilCallbacksMeanNone(t *testing.T) {
	s := drainServer(t)
	c := NewClient(WithDSN(s.URL))
	for name, opt := range map[string]InsertOption{
		"WithFormatWriter(nil)":        WithFormatWriter(nil),
		"WithFormatWriterContext(nil)": WithFormatWriterContext(nil),
		"WithBodyWriter(nil)":          WithBodyWriter(nil),
		"WithBodyWriterContext(nil)":   WithBodyWriterContext(nil),
	} {
		if err := c.Insert(t.Context(), "t", opt); err == nil {
			t.Errorf("Insert with %s: nil error, want missing-writer error", name)
		}
	}
	for name, opt := range map[string]SelectOption{
		"WithFormatReader(nil)":        WithFormatReader(nil),
		"WithFormatReaderContext(nil)": WithFormatReaderContext(nil),
		"WithBodyReader(nil)":          WithBodyReader(nil),
		"WithBodyReaderContext(nil)":   WithBodyReaderContext(nil),
	} {
		if err := c.Select(t.Context(), "SELECT", opt); err != nil {
			t.Errorf("Select with %s: %v", name, err)
		}
	}
}
