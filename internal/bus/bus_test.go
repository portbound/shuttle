package bus

import (
	"testing"
)

func TestMsgBus_Publish(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		payload []byte
		wantErr bool
	}{
		{
			name:    "Standard Publish",
			topic:   "user-signup",
			payload: []byte(`{"id": 123}`),
			wantErr: false,
		},
		{
			name:    "Empty Topic",
			topic:   "",
			payload: []byte("some bytes"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStore()
			b := New(store)
			err := b.Publish(t.Context(), tt.topic, tt.payload)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Publish() error: %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Publish() succeeded unexpectedly")
			}
		})
	}
}

func TestMsgBus_Subscribe(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{
			name:    "Standard Subscribe",
			topic:   "user-signup",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStore()
			b := New(store)
			err := b.Subscribe(t.Context(), tt.topic)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Subscribe() error: %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Subscribe() succeeded unexpectedly")
			}
		})
	}
}
