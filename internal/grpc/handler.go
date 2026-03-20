package server

import (
	"context"

	"github.com/portbound/shuttle/internal/bus"
	pb "github.com/portbound/shuttle/proto"
	"google.golang.org/grpc"
)

type Handler struct {
	pb.UnimplementedShuttleServer
	bus *bus.Bus
}

func New(b *bus.Bus) *Handler {
	return &Handler{bus: b}
}

func (h *Handler) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	err := h.bus.Publish(ctx, req.Topic, req.Payload)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (h *Handler) HealthCheck(context.Context, *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return nil, nil
}

func (h *Handler) ListTopics(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	return nil, nil
}

func (h *Handler) Subscribe(req *pb.SubscribeRequest, stream grpc.ServerStreamingServer[pb.SubscribeResponse]) error {
	ch, err := h.bus.Subscribe(stream.Context(), req.GroupId, req.Topic)
	if err != nil {
		return err
	}

	for e := range ch {
		stream.Send(&pb.SubscribeResponse{
			MessageId: e.Id,
			Topic:     e.Topic,
			Payload:   e.Payload,
			Timestamp: e.Timestamp.UnixNano(),
		})
	}

	// TODO: what kind of error are we returning?
	return nil
}
