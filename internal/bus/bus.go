package bus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Store interface {
	Save(ctx context.Context, e *Event) error
}

var (
	ErrEmptyTopic = errors.New("topic cannot be empty")
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
			log.Printf("Critical: Group %s is busy", group)
		}
	}

	return nil
}

func (b *Bus) Subscribe(ctx context.Context, topic, groupId string) (chan *Event, error) {
	if topic == "" {
		return nil, ErrEmptyTopic
	}

	sub := &subscriber{
		id: uuid.NewString(),
		ch: make(chan *Event, 100), // TODO: need to look into buffered/vs non buffered implication here
	}

	b.mu.Lock()
	if _, ok := b.registry[topic]; !ok {
		b.registry[topic] = make(map[string][]*subscriber)
	}
	b.registry[topic][groupId] = append(b.registry[topic][groupId], sub)
	b.mu.Unlock()

	go func() {
		<-ctx.Done()
		b.mu.Lock()
		defer b.mu.Unlock()

		subs := b.registry[topic][groupId]
		for i, s := range subs {
			if s.id == sub.id {
				subs[i] = subs[len(subs)-1]
				b.registry[topic][groupId] = subs[:len(subs)-1]
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
