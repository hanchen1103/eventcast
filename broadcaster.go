package eventcast

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Broadcaster[T any] struct {
	mu          sync.RWMutex
	nextID      uint64
	subscribers map[uint64]*Subscription[T]
	closed      bool

	seq       atomic.Uint64
	published atomic.Uint64
	dropped   atomic.Uint64
}

func New[T any]() *Broadcaster[T] {
	return &Broadcaster[T]{
		subscribers: make(map[uint64]*Subscription[T]),
	}
}

func (b *Broadcaster[T]) Subscribe(opts ...SubscribeOption) (*Subscription[T], error) {
	options := defaultSubscribeOptions()
	for _, opt := range opts {
		opt(&options)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrClosed
	}

	id := b.nextID
	b.nextID++

	sub := &Subscription[T]{
		id:     id,
		ch:     make(chan Envelope[T], options.buffer),
		parent: b,
		done:   make(chan struct{}),
		buffer: options.buffer,
		policy: options.policy,
	}
	b.subscribers[id] = sub
	return sub, nil
}

func (b *Broadcaster[T]) Publish(ctx context.Context, event T) error {
	if ctx == nil {
		ctx = context.Background()
	}

	env := Envelope[T]{
		Seq:       b.seq.Add(1),
		Published: time.Now(),
		Event:     event,
	}

	subscribers, err := b.snapshotSubscribers()
	if err != nil {
		return err
	}

	for _, sub := range subscribers {
		if err := b.deliver(ctx, sub, env); err != nil {
			return err
		}
	}
	b.published.Add(1)
	return nil
}

func (b *Broadcaster[T]) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true

	subscribers := make([]*Subscription[T], 0, len(b.subscribers))
	for _, sub := range b.subscribers {
		subscribers = append(subscribers, sub)
	}
	b.subscribers = nil
	b.mu.Unlock()

	for _, sub := range subscribers {
		sub.close()
	}
}

func (b *Broadcaster[T]) Stats() BroadcasterStats {
	b.mu.RLock()
	closed := b.closed
	subscribers := len(b.subscribers)
	b.mu.RUnlock()

	return BroadcasterStats{
		Closed:      closed,
		Published:   b.published.Load(),
		Subscribers: subscribers,
		Dropped:     b.dropped.Load(),
	}
}

func (b *Broadcaster[T]) snapshotSubscribers() ([]*Subscription[T], error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrClosed
	}

	subscribers := make([]*Subscription[T], 0, len(b.subscribers))
	for _, sub := range b.subscribers {
		subscribers = append(subscribers, sub)
	}
	return subscribers, nil
}

func (b *Broadcaster[T]) unsubscribe(id uint64) {
	b.mu.Lock()
	if b.subscribers != nil {
		delete(b.subscribers, id)
	}
	b.mu.Unlock()
}

func (b *Broadcaster[T]) deliver(ctx context.Context, sub *Subscription[T], env Envelope[T]) error {
	if !sub.beginDelivery() {
		return nil
	}
	defer sub.endDelivery()

	switch sub.policy {
	case DropLatest:
		select {
		case <-sub.done:
		case sub.ch <- env:
			sub.lastSeq.Store(env.Seq)
		default:
			sub.dropped.Add(1)
			b.dropped.Add(1)
		}
	case DropOldest:
		select {
		case <-sub.done:
			return nil
		case sub.ch <- env:
			sub.lastSeq.Store(env.Seq)
		default:
			select {
			case <-sub.done:
				return nil
			case <-sub.ch:
				sub.dropped.Add(1)
				b.dropped.Add(1)
			default:
			}
			select {
			case <-sub.done:
			case sub.ch <- env:
				sub.lastSeq.Store(env.Seq)
			default:
				sub.dropped.Add(1)
				b.dropped.Add(1)
			}
		}
	default:
		select {
		case <-sub.done:
		case sub.ch <- env:
			sub.lastSeq.Store(env.Seq)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
