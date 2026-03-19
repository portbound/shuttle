package bus

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mu:   sync.RWMutex{},
		data: make(map[string][][]byte),
	}
}

func (m *MemoryStore) Save(ctx context.Context, e *Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[e.Topic] = append(m.data[e.Topic], e.Payload)
	return nil
}
