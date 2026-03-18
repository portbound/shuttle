package bus

import (
	"context"
	"errors"
	"sync"
)

type Store interface {
	Save(ctx context.Context, topic string, data []byte) error
}

type Bus struct {
	sync.RWMutex
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
		return errors.New("topic cannot be empty")
	}

	if err := b.store.Save(ctx, topic, data); err != nil {
		return err
	}

	b.RLock()
	defer b.RUnlock()
	for _, ch := range b.subscribers[topic] {
		ch <- data
	}

	return nil
}

func (b *Bus) Subscribe(ctx context.Context, topic string) error {
	ch := make(chan []byte)

	b.Lock()
	b.subscribers[topic] = append(b.subscribers[topic], ch)
	b.Unlock()

	go func() {
		<-ctx.Done()

		b.Lock()
		defer b.Unlock()

		// delete the channel from the slice
		close(ch)
	}()

	return nil
}
