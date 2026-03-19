package bus

import (
	"testing"
)

func TestMsgBus_Publish(t *testing.T) {
	tests := []struct {
		name             string
		topic            string
		payload          []byte
		wantErr          bool
		setupSubscribers func(t *testing.T, b *Bus)
	}{
		{
			name:             "Standard Publish",
			topic:            "user-signup",
			payload:          []byte(`{"id": 123}`),
			wantErr:          false,
			setupSubscribers: func(t *testing.T, b *Bus) {},
		},
		{
			name:             "Empty Topic",
			topic:            "",
			payload:          []byte("some bytes"),
			wantErr:          true,
			setupSubscribers: func(t *testing.T, b *Bus) {},
		},
		{
			name:    "All members busy",
			topic:   "user-signup",
			payload: []byte("some bytes"),
			wantErr: true,
			setupSubscribers: func(t *testing.T, b *Bus) {
				b.Subscribe(t.Context(), "user-signup", "user-signup-group")
			},
		},
		{
			name:    "One member available",
			topic:   "user-signup",
			payload: []byte("some bytes"),
			wantErr: true,
			setupSubscribers: func(t *testing.T, b *Bus) {
				b.Subscribe(t.Context(), "user-signup", "user-signup-group")
				b.Subscribe(t.Context(), "user-signup", "user-signup-group")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStore()
			b := New(store)
			tt.setupSubscribers(t, b)

			// TODO: we need to figure out whether this should be buffered or unbuffered. This changes the behavior of the test. Unbuffered channels will not accept messages unless we explicitly receive from them. Buffered channels will receive until the buffer is full. I think generally it's better to use buffered in prod, but using unbuffered makes the tests easier.
			err := b.Publish(t.Context(), tt.topic, tt.payload)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Publish() error: %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("Publish() succeeded unexpectedly")
			}
		})
	}
}

func TestMsgBus_Subscribe(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		groupId string
		wantErr bool
	}{
		{
			name:    "Standard Subscribe",
			topic:   "user-signup",
			groupId: "new-group",
			wantErr: false,
		},
		{
			name:    "Empty Topic",
			topic:   "",
			groupId: "new-group",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStore()
			b := New(store)
			_, err := b.Subscribe(t.Context(), tt.topic, tt.groupId)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("%s: %v", tt.name, err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("%s succeeded unexpectedly", tt.name)
			}
		})
	}
}
