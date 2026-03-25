package shuttle

import (
	"context"
	"errors"
	"fmt"
	"io"

	pb "github.com/portbound/shuttle/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const MaxPayloadSize = 256 * 1024
const Service = "shuttle.Shuttle"

type ShuttleServingStatus int

const (
	StatusUnknown ShuttleServingStatus = iota
	StatusServing
	StatusNotServing
)

func (s ShuttleServingStatus) String() string {
	return [...]string{"StatusUnknown", "StatusServing", "StatusNotServing"}[s]
}

var (
	ErrEmptyTopic      = errors.New("topic cannot be empty")
	ErrPayloadTooLarge = errors.New("payload exceeds MaxPayloadSize")
	ErrGroupBusy       = errors.New("consumers are fully saturated")
)

var serviceConfig = `{
            "methodConfig": [{
        		"name": [{"service": "shuttle.Shuttle"}],
                "retryPolicy": {
                    "MaxAttempts": 4,
                    "InitialBackoff": ".01s",
                    "MaxBackoff": ".01s",
                    "BackoffMultiplier": 1.0,
                    "RetryableStatusCodes": [ "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "UNAVAILABLE" ]
                }
            }]
        }`

type Message struct {
	MessageId string
	Payload   []byte
	Timestamp int64
	Err       error
}

type HealthCheck struct {
	Status ShuttleServingStatus
	Err    error
}

type options struct {
	ctx   context.Context
	creds credentials.TransportCredentials
}

type Option func(*options)

func WithTLS(c credentials.TransportCredentials) Option {
	return func(o *options) {
		o.creds = c
	}
}

type Client interface {
	Publish(ctx context.Context, topic string, data []byte) (string, error)
	Subscribe(ctx context.Context, topic, group string) (<-chan *Message, error)
	ListTopics(ctx context.Context) ([]string, error)
	CheckHealth(ctx context.Context) (ShuttleServingStatus, error)
	WatchHealth(ctx context.Context) (<-chan HealthCheck, error)
	Close() error
}

type client struct {
	shuttleClient pb.ShuttleClient
	healthClient  grpc_health_v1.HealthClient
	conn          *grpc.ClientConn
}

func New(addr string, opts ...Option) (Client, error) {
	cfg := &options{
		creds: insecure.NewCredentials(),
	}

	for _, opt := range opts {
		opt(cfg)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(cfg.creds), grpc.WithDefaultServiceConfig(serviceConfig))
	if err != nil {
		return nil, fmt.Errorf("set up client: %v", err)
	}

	shuttleClient := pb.NewShuttleClient(conn)
	healthClient := grpc_health_v1.NewHealthClient(conn)

	return &client{
		shuttleClient: shuttleClient,
		healthClient:  healthClient,
		conn:          conn,
	}, nil
}

func (c *client) Publish(ctx context.Context, topic string, data []byte) (string, error) {
	if topic == "" {
		return "", ErrEmptyTopic
	}

	if len(data) > MaxPayloadSize {
		return "", ErrPayloadTooLarge
	}

	req := &pb.PublishRequest{
		Topic:   topic,
		Payload: data,
	}

	resp, err := c.shuttleClient.Publish(ctx, req)
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.ResourceExhausted {
			return "", ErrGroupBusy
		}
		return "", err
	}

	return resp.MessageId, nil
}

func (c *client) Subscribe(ctx context.Context, topic, group string) (<-chan *Message, error) {
	if topic == "" {
		return nil, ErrEmptyTopic
	}

	req := &pb.SubscribeRequest{
		Topic: topic,
		Group: group,
	}

	stream, err := c.shuttleClient.Subscribe(ctx, req)
	if err != nil {
		return nil, err
	}

	ch := make(chan *Message)
	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				event, err := stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return
					}
					ch <- &Message{
						MessageId: "",
						Payload:   nil,
						Timestamp: 0,
						Err:       err,
					}
					return
				}

				ch <- &Message{
					MessageId: event.MessageId,
					Payload:   event.Payload,
					Timestamp: event.Timestamp,
					Err:       nil,
				}
			}
		}
	}()

	return ch, nil
}

func (c *client) ListTopics(ctx context.Context) ([]string, error) {
	req := &pb.ListTopicsRequest{}
	resp, err := c.shuttleClient.ListTopics(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Topics, nil
}

func (c *client) CheckHealth(ctx context.Context) (ShuttleServingStatus, error) {
	req := &grpc_health_v1.HealthCheckRequest{Service: Service}

	resp, err := c.healthClient.Check(ctx, req)
	if err != nil {
		return StatusUnknown, err
	}

	switch resp.Status {
	case grpc_health_v1.HealthCheckResponse_SERVING:
		return StatusServing, nil
	case grpc_health_v1.HealthCheckResponse_NOT_SERVING:
		return StatusNotServing, nil
	default:
		return StatusUnknown, nil
	}
}

func (c *client) WatchHealth(ctx context.Context) (<-chan HealthCheck, error) {
	req := &grpc_health_v1.HealthCheckRequest{Service: Service}

	stream, err := c.healthClient.Watch(ctx, req)
	if err != nil {
		return nil, err
	}

	ch := make(chan HealthCheck, 1)
	go func() {
		defer close(ch)
		var status ShuttleServingStatus

		for {
			select {
			case <-ctx.Done():
				return
			default:
				resp, err := stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return
					}

					ch <- HealthCheck{
						Status: status,
						Err:    nil,
					}
					return
				}

				switch resp.Status {
				case grpc_health_v1.HealthCheckResponse_SERVING:
					status = StatusServing
				case grpc_health_v1.HealthCheckResponse_NOT_SERVING:
					status = StatusNotServing
				default:
					status = StatusUnknown
				}

				select {
				case <-ch:
				default:
				}

				ch <- HealthCheck{
					Status: status,
					Err:    nil,
				}
			}
		}
	}()

	return ch, nil
}

func (c *client) Close() error {
	return c.conn.Close()
}

// TODO: Hookup TLS
// TODO: Want to inject silo as perm storage option
// TODO: When calling an rpc, if server is not online, we should probably back off and wait a moment before trying again immediately. I think this is exponential backoff.
