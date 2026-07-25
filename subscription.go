package eventcast

import (
	"sync"
	"sync/atomic"
)

type Subscription[T any] struct {
	id     uint64
	ch     chan Envelope[T]
	parent *Broadcaster[T]
	once   sync.Once
	done   chan struct{}

	buffer  int
	policy  DeliveryPolicy
	dropped atomic.Uint64
	lastSeq atomic.Uint64
	closed  atomic.Bool

	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup
}

func (s *Subscription[T]) C() <-chan Envelope[T] {
	return s.ch
}

func (s *Subscription[T]) Close() {
	s.parent.unsubscribe(s.id)
	s.close()
}

func (s *Subscription[T]) close() {
	s.once.Do(s.markClosed)
}

func (s *Subscription[T]) Stats() SubscriptionStats {
	return SubscriptionStats{
		ID:      s.id,
		Buffer:  s.buffer,
		Len:     len(s.ch),
		Dropped: s.dropped.Load(),
		LastSeq: s.lastSeq.Load(),
		Closed:  s.closed.Load(),
	}
}

func (s *Subscription[T]) markClosed() {
	s.mu.Lock()
	if !s.closing {
		s.closing = true
		close(s.done)
	}
	s.mu.Unlock()

	s.wg.Wait()
	s.closed.Store(true)
	close(s.ch)
}

func (s *Subscription[T]) beginDelivery() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return false
	}
	s.wg.Add(1)
	return true
}

func (s *Subscription[T]) endDelivery() {
	s.wg.Done()
}
