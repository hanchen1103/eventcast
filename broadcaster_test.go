package eventcast

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPublishFanout(t *testing.T) {
	b := New[int]()

	a, err := b.Subscribe(WithBuffer(1))
	if err != nil {
		t.Fatalf("subscribe a: %v", err)
	}
	c, err := b.Subscribe(WithBuffer(1))
	if err != nil {
		t.Fatalf("subscribe c: %v", err)
	}

	if err := b.Publish(context.Background(), 42); err != nil {
		t.Fatalf("publish: %v", err)
	}

	assertEnvelope(t, <-a.C(), 1, 42)
	assertEnvelope(t, <-c.C(), 1, 42)
}

func TestDropLatest(t *testing.T) {
	b := New[int]()
	sub, err := b.Subscribe(WithBuffer(1), WithPolicy(DropLatest))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := b.Publish(context.Background(), 1); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	if err := b.Publish(context.Background(), 2); err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	assertEnvelope(t, <-sub.C(), 1, 1)
	if got := sub.Stats().Dropped; got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
}

func TestDropOldest(t *testing.T) {
	b := New[int]()
	sub, err := b.Subscribe(WithBuffer(1), WithPolicy(DropOldest))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := b.Publish(context.Background(), 1); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	if err := b.Publish(context.Background(), 2); err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	assertEnvelope(t, <-sub.C(), 2, 2)
	if got := sub.Stats().Dropped; got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
}

func TestClose(t *testing.T) {
	b := New[int]()
	sub, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	sub.Close()
	sub.Close()

	if _, ok := <-sub.C(); ok {
		t.Fatal("subscription channel should be closed")
	}
	if got := b.Stats().Subscribers; got != 0 {
		t.Fatalf("subscribers = %d, want 0", got)
	}

	b.Close()
	b.Close()

	if err := b.Publish(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close error = %v, want ErrClosed", err)
	}
	if _, err := b.Subscribe(); !errors.Is(err, ErrClosed) {
		t.Fatalf("subscribe after close error = %v, want ErrClosed", err)
	}
}

func TestCloseSubscriptionUnblocksBlockedPublish(t *testing.T) {
	b := New[int]()
	sub, err := b.Subscribe(WithBuffer(0), WithPolicy(Block))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	published := make(chan error, 1)
	go func() {
		published <- b.Publish(context.Background(), 1)
	}()

	sub.Close()

	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("publish error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("publish did not unblock after subscription close")
	}
}

func assertEnvelope[T comparable](t *testing.T, got Envelope[T], wantSeq uint64, wantEvent T) {
	t.Helper()
	if got.Seq != wantSeq {
		t.Fatalf("seq = %d, want %d", got.Seq, wantSeq)
	}
	if got.Event != wantEvent {
		t.Fatalf("event = %v, want %v", got.Event, wantEvent)
	}
}
