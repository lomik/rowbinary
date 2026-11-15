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

func TestInsertEarlySuccessWaitsForWriter(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = http.NewResponseController(w).EnableFullDuplex()
		<-started
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
	}))
	defer s.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	sentinel := errors.New("late writer failure")
	done := make(chan error, 1)
	go func() {
		done <- NewClient(WithDSN(s.URL)).Insert(ctx, "data", WithBodyWriter(func(w io.Writer) error {
			close(started)
			<-release
			defer close(finished)
			return sentinel
		}))
	}()
	select {
	case err := <-done:
		close(release)
		<-finished
		t.Fatalf("Insert returned before writer completed: %v", err)
	case <-time.After(200 * time.Millisecond):
		close(release)
		if err := <-done; !errors.Is(err, sentinel) {
			t.Fatalf("got %v, want writer error", err)
		}
	}
}

type cancelAfterBodyTransport struct {
	cancel context.CancelFunc
}

func (tr cancelAfterBodyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if _, err := io.Copy(io.Discard, req.Body); err != nil {
		return nil, err
	}
	// The upload is complete; cancel only after the producer reported success.
	tr.cancel()
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(http.NoBody),
		Request:    req,
	}, nil
}

func TestInsertSuccessNotMaskedByLateCancel(t *testing.T) {
	for i := range 100 {
		ctx, cancel := context.WithCancel(t.Context())
		c := NewClient(
			WithDSN("http://127.0.0.1:1"),
			WithHTTPClient(&http.Client{Transport: cancelAfterBodyTransport{cancel: cancel}}),
		)
		err := c.Insert(ctx, "data", WithBodyWriter(func(w io.Writer) error {
			_, err := w.Write([]byte("payload"))
			return err
		}))
		cancel()
		if err != nil {
			t.Fatalf("iteration %d: successful upload reported %v", i, err)
		}
	}
}
