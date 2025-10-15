package rowbinary

import (
	"context"
	"net/http"
	"sync"
)

var globalDiscovery = &struct {
	sync.RWMutex
	callback func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error)
}{}

var globalHTTPClient = &struct {
	sync.RWMutex
	client *http.Client
}{}

func SetGlobalDiscovery(callback func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error)) {
	globalDiscovery.Lock()
	defer globalDiscovery.Unlock()
	globalDiscovery.callback = callback
}

func SetGlobalHTTPClient(client *http.Client) {
	globalHTTPClient.Lock()
	defer globalHTTPClient.Unlock()
	globalHTTPClient.client = client
}

func getGlobalHTTPClient() *http.Client {
	globalHTTPClient.RLock()
	defer globalHTTPClient.RUnlock()
	return globalHTTPClient.client
}

func getGlobalDiscovery() func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error) {
	globalDiscovery.RLock()
	defer globalDiscovery.RUnlock()
	return globalDiscovery.callback
}
