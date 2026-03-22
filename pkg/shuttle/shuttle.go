package shuttle

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/portbound/shuttle/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const MaxPayloadSize = 256 * 1024
const Service = "shuttle.Shuttle"

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

type Client struct {
	shuttleClient pb.ShuttleClient
	healthClient  grpc_health_v1.HealthClient
	conn          *grpc.ClientConn
}

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

// target := os.Getenv("SHUTTLE_SERVER_ADDR")
//
//	if target == "" {
//		target = "localhost:50051"
//	}
//
// addr := fmt.Sprintf("dns:///%s", target)

func New(addr string, opts ...Option) (*Client, error) {
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

	return &Client{
		shuttleClient: shuttleClient,
		healthClient:  healthClient,
		conn:          conn,
	}, nil
}

func (c *Client) Publish(ctx context.Context, topic string, data []byte) (*pb.PublishResponse, error) {
	// TODO: validate user input -> need to move sentinel errs out of bus.go

	req := &pb.PublishRequest{
		Topic:   topic,
		Payload: data,
	}

	return c.shuttleClient.Publish(ctx, req)
}

func (c *Client) Subscribe(ctx context.Context, topic, group string) (grpc.ServerStreamingClient[pb.SubscribeResponse], error) {
	req := &pb.SubscribeRequest{
		Topic: topic,
		Group: group,
	}

	return c.shuttleClient.Subscribe(ctx, req)
}

func (c *Client) ListTopics(ctx context.Context, in *pb.ListTopicsRequest, opts ...grpc.CallOption) (*pb.ListTopicsResponse, error) {
	req := &pb.ListTopicsRequest{}
	return c.shuttleClient.ListTopics(ctx, req)
}

func (c *Client) CheckHealth(ctx context.Context) (grpc_health_v1.HealthCheckResponse_ServingStatus, error) {
	req := &grpc_health_v1.HealthCheckRequest{Service: Service}

	res, err := c.healthClient.Check(ctx, req)
	if err != nil {
		return grpc_health_v1.HealthCheckResponse_UNKNOWN, err
	}

	return res.Status, nil
}

func (c *Client) WatchHealth(ctx context.Context) (grpc_health_v1.Health_WatchClient, error) {
	req := &grpc_health_v1.HealthCheckRequest{Service: Service}
	return c.healthClient.Watch(ctx, req)
}
