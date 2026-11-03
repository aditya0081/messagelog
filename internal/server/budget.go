package server

import (
	"context"
	"sync"
)

// inFlightBudget bounds the requests one connection has read but not yet
// answered, by count and by bytes. Only the connection's read loop
// acquires; any goroutine may release.
type inFlightBudget struct {
	maxRequests int
	maxBytes    int

	mu       sync.Mutex
	requests int
	bytes    int

	// released wakes the read loop while it waits for room. It holds at
	// most one token: a release that finds a token already there adds
	// nothing, because the one waiter rechecks the totals whenever it wakes.
	released chan struct{}
}

func newInFlightBudget(maxRequests, maxBytes int) *inFlightBudget {
	return &inFlightBudget{maxRequests: maxRequests, maxBytes: maxBytes, released: make(chan struct{}, 1)}
}

// acquire reserves room for one request of size bytes, waiting until there
// is room. It returns false if ctx ends first. size must be at most
// maxBytes, or it never fits.
func (b *inFlightBudget) acquire(ctx context.Context, size int) bool {
	for {
		if b.tryAcquire(size) {
			return true
		}
		select {
		case <-b.released:
		case <-ctx.Done():
			return false
		}
	}
}

func (b *inFlightBudget) tryAcquire(size int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.requests >= b.maxRequests || b.bytes+size > b.maxBytes {
		return false
	}
	b.requests++
	b.bytes += size
	return true
}

// release returns one request of size bytes.
func (b *inFlightBudget) release(size int) {
	b.mu.Lock()
	b.requests--
	b.bytes -= size
	b.mu.Unlock()
	select {
	case b.released <- struct{}{}:
	default:
	}
}
