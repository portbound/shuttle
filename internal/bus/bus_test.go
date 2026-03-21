package bus

import (
	"slices"
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
			name:    "Standard Publish",
			topic:   "user-signup",
			payload: []byte(`{"id": 123}`),
			wantErr: false,
			setupSubscribers: func(t *testing.T, b *Bus) {
				ch, _ := b.Subscribe(t.Context(), "user-signup", "user-signup-group")
				go func() {
					<-ch
				}()
			},
		},
		{
			name:    "One subscriber available",
			topic:   "user-signup",
			payload: []byte("some bytes"),
			wantErr: false,
			setupSubscribers: func(t *testing.T, b *Bus) {
				ch1, _ := b.Subscribe(t.Context(), "user-signup", "user-signup-group")
				ch1 <- &Event{}

				ch2, _ := b.Subscribe(t.Context(), "user-signup", "user-signup-group")
				go func() {
					<-ch2
				}()
			},
		},
		{
			name:    "All subscribers busy",
			topic:   "user-signup",
			payload: []byte("some bytes"),
			wantErr: true,
			setupSubscribers: func(t *testing.T, b *Bus) {
				ch, _ := b.Subscribe(t.Context(), "user-signup", "user-signup-group")
				ch <- &Event{}
			},
		},
		{
			name:             "Empty Topic",
			topic:            "",
			payload:          []byte("some bytes"),
			wantErr:          true,
			setupSubscribers: func(t *testing.T, b *Bus) {},
		},
		{
			name:             "Payload too large",
			topic:            "user-signup",
			payload:          make([]byte, 257*1024),
			wantErr:          true,
			setupSubscribers: func(t *testing.T, b *Bus) {},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New()
			tt.setupSubscribers(t, b)

			_, err := b.Publish(t.Context(), tt.topic, tt.payload)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("failed unexpectedly: %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("succeeded unexpectedly")
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
			b := New()
			_, err := b.Subscribe(t.Context(), tt.topic, tt.groupId)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("failed unexpectedly: %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("%s succeeded unexpectedly", tt.name)
			}
		})
	}
}

func TestBus_Topics(t *testing.T) {
	tests := []struct {
		name    string
		want    []string
		wantErr bool
	}{
		{
			name:    "Standard Topics",
			want:    []string{"user-signup", "stock-updates", "heartbeat"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New()
			for _, topic := range tt.want {
				b.registry[topic] = make(map[string][]*subscriber)
			}
			got, err := b.Topics(t.Context())
			if err != nil {
				if !tt.wantErr {
					t.Errorf("failed unexpectedly: %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("%s succeeded unexpectedly", tt.name)
			}

			if !slices.Equal(got, tt.want) {
				t.Errorf("got: %v want: %v", got, tt.want)
			}
		})
	}
}
