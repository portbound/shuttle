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

var (
	ErrEmptyTopic      = errors.New("topic cannot be empty")
	ErrPayloadTooLarge = errors.New("payload exceeds MaxPayloadSize")
	ErrGroupBusy       = errors.New("consumers are fully saturated")
)

type Bus struct {
	mu       sync.RWMutex
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

func New() *Bus {
	return &Bus{
		registry: make(map[string]map[string][]*subscriber),
	}
}

func (b *Bus) Publish(ctx context.Context, topic string, data []byte) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	if topic == "" {
		return "", ErrEmptyTopic
	}

	if len(data) > MaxPayloadSize {
		return "", ErrPayloadTooLarge
	}

	e := &Event{
		Id:        uuid.NewString(),
		Topic:     topic,
		Timestamp: time.Now(),
		Payload:   data,
	}

	b.mu.RLock()
	groups, ok := b.registry[topic]
	b.mu.RUnlock()

	if !ok {
		return e.Id, nil
	}


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
			case <-ctx.Done():
				return "", ctx.Err()
			default:
				continue
			}

			if sent {
				break
			}
		}

		if !sent {
			return "", fmt.Errorf("%s: %w", group, ErrGroupBusy)
		}
	}

	return e.Id, nil
}

func (b *Bus) Subscribe(ctx context.Context, topic, group string) (chan *Event, error) {
	if topic == "" {
		return nil, ErrEmptyTopic
	}

	sub := &subscriber{
		id: uuid.NewString(),
		ch: make(chan *Event, 1),
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
				break
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

func (b *Bus) Topics(ctx context.Context) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var topics []string
	for topic := range b.registry {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			topics = append(topics, topic)
		}
	}

	return topics, nil
}


