package bus

import (
	"context"
	"sync"
)

type MemoryStore struct {
	sync.RWMutex
	data map[string][][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		RWMutex: sync.RWMutex{},
		data:    make(map[string][][]byte),
	}
}

func (m *MemoryStore) Save(ctx context.Context, topic string, data []byte) error {
	m.Lock()
	defer m.Unlock()
	m.data[topic] = append(m.data[topic], data)
	return nil
}
