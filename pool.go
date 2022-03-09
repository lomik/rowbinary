package rowbinary

import "sync"

// Pool ...
type Pool struct {
	sync.RWMutex
	pool       chan *Buffer
	poolSize   int
	bufferSize int
}

// NewPool ...
func NewPool(poolSize int, bufferSize int) *Pool {
	return &Pool{
		pool:       make(chan *Buffer, poolSize),
		poolSize:   poolSize,
		bufferSize: bufferSize,
	}
}

// Resize ...
func (p *Pool) Resize(poolSize int, bufferSize int) {
	p.Lock()
	if p.poolSize != poolSize || p.bufferSize != bufferSize {
		p.poolSize = poolSize
		p.bufferSize = bufferSize
		p.pool = make(chan *Buffer, poolSize)
	}
	p.Unlock()
}

// Put ...
func (p *Pool) Put(b *Buffer) {
	p.RLock()
	bufferSize := p.bufferSize
	ch := p.pool
	p.RUnlock()
	if b.Cap() != bufferSize {
		// invalid size of buffer
		return
	}
	b.Reset()
	b.pool = p

	select {
	case ch <- b:
		// pass
	default:
		// pass
	}
}

// Get ...
func (p *Pool) Get() *Buffer {
	p.RLock()
	bufferSize := p.bufferSize
	ch := p.pool
	p.RUnlock()

	select {
	case b := <-ch:
		if b.Cap() == bufferSize {
			b.Reset()
			return b
		}
	default:
		// pass
	}
	b := NewBuffer(p.bufferSize)
	b.pool = p
	return b
}
