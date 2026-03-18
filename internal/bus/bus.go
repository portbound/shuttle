package bus

import (
	"context"
	"errors"
	"sync"
)

type Store interface {
	Save(ctx context.Context, topic string, data []byte) error
}

var (
	ErrEmptyTopic = errors.New("topic cannot be empty")
)

type Bus struct {
	mu          sync.RWMutex
	store       Store
	subscribers map[string][]chan []byte
}

func New(s Store) *Bus {
	return &Bus{
		store:       s,
		subscribers: make(map[string][]chan []byte)}
}

func (b *Bus) Publish(ctx context.Context, topic string, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if topic == "" {
		return ErrEmptyTopic
	}

	if err := b.store.Save(ctx, topic, data); err != nil {
		return err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subscribers[topic] {
		ch <- data
	}

	return nil
}

func (b *Bus) Subscribe(ctx context.Context, topic string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if topic == "" {
		return ErrEmptyTopic
	}

	ch := make(chan []byte)

	b.mu.Lock()
	b.subscribers[topic] = append(b.subscribers[topic], ch)
	b.mu.Unlock()

	go func() {
		<-ctx.Done()

		b.mu.Lock()
		defer b.mu.Unlock()

		// delete the channel from the slice
		close(ch)
	}()

	return nil
}
