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
