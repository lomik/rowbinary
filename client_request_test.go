package rowbinary

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestHeaderCaseInsensitiveOverride(t *testing.T) {
	for _, method := range []string{"Exec", "Select", "Insert"} {
		t.Run(method, func(t *testing.T) {
			headers := map[string]string{}
			for _, opt := range []headerOption{WithHeader("x-audit", "default"), WithHeader("X-Audit", "request"), WithHeader("X-AUDIT", "last")} {
				switch method {
				case "Exec":
					opt.applyExecOptions(&execOptions{headers: headers})
				case "Select":
					opt.applySelectOptions(&selectOptions{headers: headers})
				case "Insert":
					opt.applyInsertOptions(&insertOptions{headers: headers})
				}
			}
			if len(headers) != 1 || headers["X-Audit"] != "last" {
				t.Fatalf("case-insensitive overrides not merged: %v", headers)
			}
		})
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Audit"); got != "request" {
			t.Errorf("wire header = %q, want request", got)
		}
	}))
	defer s.Close()
	c := NewClient(WithDSN(s.URL), WithHeader("x-audit", "default"))
	if err := c.Exec(t.Context(), "SELECT 1", WithHeader("X-AUDIT", "request")); err != nil {
		t.Fatal(err)
	}
}

func TestDSNPathAndQuery(t *testing.T) {
	for _, discovery := range []bool{false, true} {
		t.Run(fmt.Sprint(discovery), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.EscapedPath() != "/proxy%2Fclickhouse/" {
					t.Errorf("path = %q", r.URL.EscapedPath())
				}
				q := r.URL.Query()
				if q.Get("database") != "analytics" || q.Get("max_threads") != "2" || q.Get("param_x") != "request" || len(q["tag"]) != 2 {
					t.Errorf("query = %v", q)
				}
				user, pass, ok := r.BasicAuth()
				if !ok || user != "user" || pass != "password" {
					t.Error("DSN authentication lost")
				}
			}))
			defer s.Close()
			u, err := url.Parse(s.URL + "/proxy%2Fclickhouse/?database=analytics&max_threads=2&param_x=dsn&tag=a&tag=b")
			if err != nil {
				t.Fatal(err)
			}
			u.User = url.UserPassword("user", "password")
			opts := []ClientOption{WithDSN(u.String())}
			if discovery {
				opts = []ClientOption{WithDSN("http://unused.invalid"), WithDiscovery(func(context.Context, string, DiscoveryCtx) (string, error) { return u.String(), nil })}
			}
			if err := NewClient(opts...).Exec(t.Context(), "SELECT 1", WithParam("param_x", "request")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
