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
	registry map[string]map[string][]*member
}

type Event struct {
	Id        string
	Topic     string
	Timestamp time.Time
	Payload   []byte
}

type member struct {
	id string
	ch chan *Event
}

func New(s Store) *Bus {
	return &Bus{
		store:    s,
		registry: make(map[string]map[string][]*member),
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

	for id, members := range groups {
		if len(members) == 0 {
			continue
		}

		var sent bool
		startIdx := rand.Intn(len(members))

		for i := 0; i < len(members); i++ {
			idx := (startIdx + i) % len(members)
			m := members[idx]

			select {
			case m.ch <- e:
				sent = true
			default:
				continue
			}

			if sent {
				break
			}
		}

		if !sent {
			log.Printf("Critical: Group %s is busy", id)
		}
	}

	return nil
}

func (b *Bus) Subscribe(ctx context.Context, topic, groupId string) (chan *Event, error) {
	if topic == "" {
		return nil, ErrEmptyTopic
	}

	m := &member{
		id: uuid.NewString(),
		ch: make(chan *Event, 100), // TODO: need to look into buffered/vs non buffered implication here
	}

	b.mu.Lock()
	if _, ok := b.registry[topic]; !ok {
		b.registry[topic] = make(map[string][]*member)
	}
	b.registry[topic][groupId] = append(b.registry[topic][groupId], m)
	b.mu.Unlock()

	go func() {
		<-ctx.Done()
		b.mu.Lock()
		defer b.mu.Unlock()

		members := b.registry[topic][groupId]
		for i, member := range members {
			if member.id == m.id {
				members[i] = members[len(members)-1]
				b.registry[topic][groupId] = members[:len(members)-1]
			}
		}

		close(m.ch)
	}()

	return m.ch, nil
}
