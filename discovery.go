package rowbinary

import (
	"context"
	"sync"
)

var globalDiscovery = &struct {
	sync.RWMutex
	callback func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error)
}{}

func SetGlobalDiscovery(callback func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error)) {
	globalDiscovery.Lock()
	defer globalDiscovery.Unlock()
	globalDiscovery.callback = callback
}

func getGlobalDiscovery() func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error) {
	globalDiscovery.RLock()
	defer globalDiscovery.RUnlock()
	return globalDiscovery.callback
}
