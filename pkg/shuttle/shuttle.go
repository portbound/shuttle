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

type Event struct {
	MessageId string
	Payload   []byte
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

type options struct {
	ctx   context.Context
	creds credentials.TransportCredentials
}

type Option func(*options)

func WithContext(ctx context.Context) Option {
	return func(o *options) {
		o.ctx = ctx
	}
}

func WithTLS(c credentials.TransportCredentials) Option {
	return func(o *options) {
		o.creds = c
	}
}

type Client interface {
	Publish(ctx context.Context, topic string, data []byte) (string, error)
	Subscribe(ctx context.Context, topic, group string) (chan *Event, error)
	ListTopics(ctx context.Context, in *pb.ListTopicsRequest, opts ...grpc.CallOption) ([]string, error)
	CheckHealth(ctx context.Context) (grpc_health_v1.HealthCheckResponse_ServingStatus, error)
	WatchHealth(ctx context.Context) (grpc_health_v1.Health_WatchClient, error)
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
	// TODO: need to check context injection 
	// TODO: want to inject silo 

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

func (c *client) Subscribe(ctx context.Context, topic, group string) (chan *Event, error) {
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

	ch := make(chan *Event)

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
				}

				ch <- &Event{
					MessageId: event.MessageId,
					Payload:   event.Payload,
				}
			}
		}
	}()

	return ch, nil
}

func (c *client) ListTopics(ctx context.Context, in *pb.ListTopicsRequest, opts ...grpc.CallOption) ([]string, error) {
	req := &pb.ListTopicsRequest{}
	resp, err := c.shuttleClient.ListTopics(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Topics, nil
}

// TODO: need to make this signature gRPC agnostic
func (c *client) CheckHealth(ctx context.Context) (grpc_health_v1.HealthCheckResponse_ServingStatus, error) {
	req := &grpc_health_v1.HealthCheckRequest{Service: Service}

	resp, err := c.healthClient.Check(ctx, req)
	if err != nil {
		return grpc_health_v1.HealthCheckResponse_UNKNOWN, err
	}

	return resp.Status, nil
}

// TODO: need to make this signature gRPC agnostic
func (c *client) WatchHealth(ctx context.Context) (grpc_health_v1.Health_WatchClient, error) {
	req := &grpc_health_v1.HealthCheckRequest{Service: Service}
	return c.healthClient.Watch(ctx, req)
}

func (c *client) Close() error {
	return c.conn.Close()
}
