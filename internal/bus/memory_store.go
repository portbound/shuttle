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

func (m *MemoryStore) Save(ctx context.Context, topic string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[topic] = append(m.data[topic], data)
	return nil
}
