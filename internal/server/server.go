package server

import (
	"context"
	"errors"

	"github.com/portbound/shuttle/internal/bus"
	pb "github.com/portbound/shuttle/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedShuttleServer
	bus *bus.Bus
}

func New(b *bus.Bus) *Server {
	return &Server{bus: b}
}

func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	msgId, err := s.bus.Publish(ctx, req.Topic, req.Payload)
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			return nil, status.Error(codes.Canceled, err.Error())
		case errors.Is(err, context.DeadlineExceeded):
			return nil, status.Error(codes.DeadlineExceeded, err.Error())
		case errors.Is(err, bus.ErrEmptyTopic), errors.Is(err, bus.ErrPayloadTooLarge):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, bus.ErrGroupBusy):
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &pb.PublishResponse{
		MessageId: msgId,
	}, nil
}

func (s *Server) Subscribe(req *pb.SubscribeRequest, stream grpc.ServerStreamingServer[pb.SubscribeResponse]) error {
	ch, err := s.bus.Subscribe(stream.Context(), req.Topic, req.Group)
	if err != nil {
		switch {
		case errors.Is(err, bus.ErrEmptyTopic):
			return status.Error(codes.InvalidArgument, err.Error())
		default:
			return status.Error(codes.Internal, err.Error())
		}
	}

	for e := range ch {
		stream.Send(&pb.SubscribeResponse{
			MessageId: e.Id,
			Topic:     e.Topic,
			Payload:   e.Payload,
			Timestamp: e.Timestamp.UnixNano(),
		})
	}

	return nil
}

func (s *Server) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	topics, err := s.bus.Topics(ctx)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, status.Error(codes.DeadlineExceeded, err.Error())
		case errors.Is(err, context.Canceled):
			return nil, status.Error(codes.Canceled, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &pb.ListTopicsResponse{
		Topics: topics,
	}, nil
}
