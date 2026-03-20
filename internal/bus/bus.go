package bus

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
)

const MaxPayloadSize = 256 * 1024

type Store interface {
	Save(ctx context.Context, e *Event) error
}

var (
	ErrEmptyTopic      = errors.New("topic cannot be empty")
	ErrPayloadTooLarge = errors.New("payload exceeds MaxPayloadSize")
	ErrGroupBusy       = errors.New("consumers are fully saturated")
	ErrNoSubscribers   = errors.New("no subscribers for topic")
)

type Bus struct {
	mu       sync.RWMutex
	store    Store
	registry map[string]map[string][]*subscriber
}

type Event struct {
	Id        string
	Topic     string
	Timestamp time.Time
	Payload   []byte
}

type subscriber struct {
	id string
	ch chan *Event
}

func New(s Store) *Bus {
	return &Bus{
		store:    s,
		registry: make(map[string]map[string][]*subscriber),
	}
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

	if len(data) > MaxPayloadSize {
		return ErrPayloadTooLarge
	}
	e := &Event{
		Id:        uuid.NewString(),
		Topic:     topic,
		Timestamp: time.Now(),
		Payload:   data,
	}

	if err := b.store.Save(ctx, e); err != nil {
		return fmt.Errorf("save event: %v", err)
	}

	b.mu.RLock()
	groups := b.registry[topic]
	b.mu.RUnlock()

	for group, subs := range groups {
		if len(subs) == 0 {
			continue
		}

		var sent bool
		startIdx := rand.Intn(len(subs))

		for i := range subs {
			idx := (startIdx + i) % len(subs)
			sub := subs[idx]

			select {
			case sub.ch <- e:
				sent = true
			default:
				continue
			}

			if sent {
				break
			}
		}

		if !sent {
			return fmt.Errorf("%s: %s", group, ErrGroupBusy)
		}
	}

	return nil
}

func (b *Bus) Subscribe(ctx context.Context, topic, group string) (chan *Event, error) {
	if topic == "" {
		return nil, ErrEmptyTopic
	}

	sub := &subscriber{
		id: uuid.NewString(),
		ch: make(chan *Event, 1),
		// need to use a buffered channel here to protect against a potential race condition. It's possiblethat as we add the first subscriber to a group, an *Event is published to the corresponding topic before Subscribe() returns to the caller and starts receiving on ch. Publish should prevent a deadlock with the default case in select, but then we'd end up logging an erroneous ErrGroupBusy
	}

	b.mu.Lock()
	if _, ok := b.registry[topic]; !ok {
		b.registry[topic] = make(map[string][]*subscriber)
	}
	b.registry[topic][group] = append(b.registry[topic][group], sub)
	b.mu.Unlock()

	go func() {
		<-ctx.Done()
		b.mu.Lock()
		defer b.mu.Unlock()

		subs := b.registry[topic][group]
		for i, s := range subs {
			if s.id == sub.id {
				subs[i] = subs[len(subs)-1]
				b.registry[topic][group] = subs[:len(subs)-1]
			}
		}

		if len(b.registry[topic][group]) == 0 {
			delete(b.registry[topic], group)
		}

		if len(b.registry[topic]) == 0 {
			delete(b.registry, topic)
		}

		close(sub.ch)
	}()

	return sub.ch, nil
}
