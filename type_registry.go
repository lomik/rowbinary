package rowbinary

import (
	"sync"
)

// type registry used for create uniq integer type ids for fast compare

var typeRegistry = struct {
	sync.RWMutex
	m  map[string]uint64
	id uint64
}{
	m: make(map[string]uint64),
}

// BinaryTypeID returns unique integer type id for binary representation
func BinaryTypeID(value []byte) uint64 {
	typeRegistry.RLock()
	id, ok := typeRegistry.m[string(value)]
	typeRegistry.RUnlock()
	if ok {
		return id
	}

	typeRegistry.Lock()
	defer typeRegistry.Unlock()

	id, ok = typeRegistry.m[string(value)]
	if ok {
		return id
	}

	typeRegistry.id++
	typeRegistry.m[string(value)] = typeRegistry.id
	return typeRegistry.id
}
