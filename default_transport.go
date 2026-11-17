package rowbinary

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// Default transport settings for clients created without WithHTTPClient or
// SetGlobalHTTPClient. They follow net/http's DefaultTransport, except for the
// larger I/O buffers and more idle connections per host (ClickHouse clients
// usually run many concurrent queries against few hosts).
const (
	defaultDialTimeout           = 30 * time.Second
	defaultKeepAlive             = 30 * time.Second
	defaultTLSHandshakeTimeout   = 10 * time.Second
	defaultIdleConnTimeout       = 90 * time.Second
	defaultExpectContinueTimeout = 1 * time.Second
	defaultMaxIdleConns          = 100
	defaultMaxIdleConnsPerHost   = 16
	defaultIOBufferSize          = 1024 * 1024
)

// defaultHTTPClient is shared by every client that does not bring its own, so
// connections are pooled across clients instead of each NewClient opening a
// separate pool. It has no overall timeout: ClickHouse may take minutes to
// send the first byte of a heavy query, so request lifetime is the caller's
// context.
var defaultHTTPClient = sync.OnceValue(func() *http.Client {
	return &http.Client{Transport: newDefaultTransport()}
})

func newDefaultTransport() *http.Transport {
	dialer := &net.Dialer{Timeout: defaultDialTimeout, KeepAlive: defaultKeepAlive}
	return &http.Transport{
		// Honors HTTP_PROXY, HTTPS_PROXY and NO_PROXY; loopback hosts never use a proxy.
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		IdleConnTimeout:       defaultIdleConnTimeout,
		ExpectContinueTimeout: defaultExpectContinueTimeout,
		MaxIdleConns:          defaultMaxIdleConns,
		MaxIdleConnsPerHost:   defaultMaxIdleConnsPerHost,
		ReadBufferSize:        defaultIOBufferSize,
		WriteBufferSize:       defaultIOBufferSize,
	}
}
