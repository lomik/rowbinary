package chtest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	for _, tc := range []struct {
		mode, dsn        string
		enabled, wantErr bool
	}{
		{"", "", false, false}, {"offline", "bad", false, false},
		{"integration", "", true, false}, {"integration", "http://u:p@localhost:1234", true, false},
		{"typo", "", false, true}, {"integration", "ftp://localhost", false, true},
		{"integration", "http://localhost/db", false, true}, {"integration", "http://localhost?query=1", false, true},
	} {
		t.Run(tc.mode+tc.dsn, func(t *testing.T) {
			enabled, s, err := Config(func(k string) string {
				if k == "ROWBINARY_TEST_MODE" {
					return tc.mode
				}
				if k == "ROWBINARY_TEST_DSN" {
					return tc.dsn
				}
				return ""
			})
			if enabled != tc.enabled || (err != nil) != tc.wantErr {
				t.Fatalf("enabled=%v error=%v", enabled, err)
			}
			if enabled && tc.dsn != "" && (s.Username != "u" || s.Password != "p") {
				t.Fatal("credentials not extracted")
			}
		})
	}
}

func TestExternalLifecycle(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "u" || p != "p" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Query().Get("query") != "SELECT 1" {
			t.Error("readiness must query ClickHouse")
		}
		fmt.Fprintln(w, "1")
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	u.User = url.UserPassword("u", "p")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	s, cleanup, err := Start(ctx, Server{DSN: u.String()})
	if err != nil {
		t.Fatal(err)
	}
	if s.DSN != u.String() {
		t.Fatal("external DSN changed")
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if err := Ready(ctx, s.DSN); err != nil {
		t.Fatalf("external server must not be stopped: %v", err)
	}
}

func TestReadinessFailureIsBounded(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusOK} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status); fmt.Fprintln(w, "not ClickHouse") }))
			defer srv.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			_, cleanup, err := Start(ctx, Server{DSN: srv.URL})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected deadline, got %v", err)
			}
			if err := cleanup(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRunLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name, mode             string
		startupErr, cleanupErr bool
		testCode, want         int
	}{
		{"offline", "offline", false, false, 0, 0},
		{"invalid mode", "typo", false, false, 0, 1},
		{"success", "integration", false, false, 0, 0},
		{"test failure", "integration", false, false, 3, 3},
		{"missing Docker", "integration", true, false, 0, 1},
		{"cleanup failure", "integration", false, true, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, cleaned, configured, ran := false, false, false, false
			code := run(func() int { ran = true; return tc.testCode }, func(s Server) { configured = true },
				func(k string) string {
					if k == "ROWBINARY_TEST_MODE" {
						return tc.mode
					}
					return ""
				},
				func(ctx context.Context, external Server) (Server, func() error, error) {
					started = true
					if _, ok := ctx.Deadline(); !ok {
						t.Error("startup must have a deadline")
					}
					cleanup := func() error {
						cleaned = true
						if tc.cleanupErr {
							return errors.New("cleanup failed")
						}
						return nil
					}
					if tc.startupErr {
						return Server{}, cleanup, errors.New("Docker unavailable")
					}
					return Server{}, cleanup, nil
				})
			if code != tc.want {
				t.Fatalf("exit=%d want=%d", code, tc.want)
			}
			if started != (tc.mode == "integration") || cleaned != started {
				t.Fatalf("started=%v cleaned=%v", started, cleaned)
			}
			wantRun := tc.mode == "offline" || tc.mode == "integration" && !tc.startupErr
			if ran != wantRun || configured != (tc.mode == "integration" && !tc.startupErr) {
				t.Fatalf("ran=%v configured=%v", ran, configured)
			}
		})
	}
}
