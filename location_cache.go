package rowbinary

import (
	"sync"
	"time"
)

type cachedLocation struct {
	loc *time.Location
	err error
}

// locationCache maps time zone names to loaded locations. time.LoadLocation
// reads zoneinfo from disk on every call, and Dynamic decodes the type of every
// value, so DateTime('tz') columns would otherwise load the zone once per row.
var locationCache sync.Map // string -> cachedLocation

// loadLocation is time.LoadLocation with a process-wide cache. Failures are
// cached too: the set of installed zones does not change while running.
func loadLocation(name string) (*time.Location, error) {
	if v, ok := locationCache.Load(name); ok {
		c := v.(cachedLocation)
		return c.loc, c.err
	}
	loc, err := time.LoadLocation(name)
	v, _ := locationCache.LoadOrStore(name, cachedLocation{loc: loc, err: err})
	c := v.(cachedLocation)
	return c.loc, c.err
}
