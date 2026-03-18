package server

import (
	"context"

	"github.com/portbound/busser/internal/bus"
	pb "github.com/portbound/busser/proto"
	"google.golang.org/grpc"
)

type Handler struct {
	pb.UnimplementedMsgBusServiceServer
	bus *bus.Bus
}

func New(b *bus.Bus) *Handler {
	return &Handler{bus: b}
}

func (h *Handler) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	return nil, nil
}

func (h *Handler) HealthCheck(context.Context, *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return nil, nil
}

func (h *Handler) ListTopics(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	return nil, nil
}

func (h *Handler) Subscribe(*pb.SubscribeRequest, grpc.ServerStreamingServer[pb.SubscribeResponse]) error {
	return nil
}
