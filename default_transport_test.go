package rowbinary

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"
)

func defaultTransportOf(t *testing.T, c Client) *http.Transport {
	t.Helper()
	tr, ok := c.(*client).opts.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T", c.(*client).opts.httpClient.Transport)
	}
	return tr
}

func TestDefaultClientsShareOneTransport(t *testing.T) {
	a, b := NewClient(), NewClient()
	if a.(*client).opts.httpClient != b.(*client).opts.httpClient {
		t.Fatal("each NewClient got its own HTTP client and connection pool")
	}
	if a.(*client).opts.httpClient.Timeout != 0 {
		t.Fatal("default client has an overall timeout; long ClickHouse queries need the caller's context")
	}

	own := &http.Client{}
	if NewClient(WithHTTPClient(own)).(*client).opts.httpClient != own {
		t.Fatal("WithHTTPClient was not honored")
	}
}

func TestDefaultTransportSettings(t *testing.T) {
	tr := defaultTransportOf(t, NewClient())
	if tr.Proxy == nil {
		t.Error("Proxy is nil: HTTP_PROXY/HTTPS_PROXY/NO_PROXY are ignored")
	}
	if tr.DialContext == nil {
		t.Error("DialContext is nil: no dial timeout")
	}
	if tr.TLSHandshakeTimeout == 0 || tr.IdleConnTimeout == 0 {
		t.Errorf("TLSHandshakeTimeout=%v IdleConnTimeout=%v, want both set", tr.TLSHandshakeTimeout, tr.IdleConnTimeout)
	}
	if tr.ResponseHeaderTimeout != 0 {
		t.Errorf("ResponseHeaderTimeout=%v; heavy queries can take minutes to send headers", tr.ResponseHeaderTimeout)
	}
	if tr.MaxIdleConnsPerHost < 16 {
		t.Errorf("MaxIdleConnsPerHost=%d, want at least 16", tr.MaxIdleConnsPerHost)
	}
	if tr.ReadBufferSize != 1<<20 || tr.WriteBufferSize != 1<<20 {
		t.Errorf("buffer sizes %d/%d, want 1 MiB", tr.ReadBufferSize, tr.WriteBufferSize)
	}
}

// TestDefaultTransportUsesEnvProxy runs a child process, because
// http.ProxyFromEnvironment reads the environment only once per process.
func TestDefaultTransportUsesEnvProxy(t *testing.T) {
	if os.Getenv("ROWBINARY_PROXY_CHILD") == "1" {
		err := NewClient(WithDSN("http://clickhouse.example.invalid:8123")).Exec(t.Context(), "SELECT 1")
		if err != nil {
			t.Fatalf("request through HTTP_PROXY failed: %v", err)
		}
		return
	}

	var mu sync.Mutex
	var proxied []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		proxied = append(proxied, r.URL.Host) // a proxy receives the absolute target URL
	}))
	defer proxy.Close()

	cmd := exec.Command(os.Args[0], "-test.run=^TestDefaultTransportUsesEnvProxy$", "-test.count=1")
	cmd.Env = append(os.Environ(), "ROWBINARY_PROXY_CHILD=1", "HTTP_PROXY="+proxy.URL, "NO_PROXY=")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(proxied) != 1 || proxied[0] != "clickhouse.example.invalid:8123" {
		t.Fatalf("proxy saw %v, want one request for clickhouse.example.invalid:8123", proxied)
	}
}
