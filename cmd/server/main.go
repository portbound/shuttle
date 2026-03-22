package main

import (
	"log"
	"net"

	"github.com/portbound/shuttle/internal/bus"
	"github.com/portbound/shuttle/internal/handler"
	pb "github.com/portbound/shuttle/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const port string = ":8080"

func main() {
	l, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("listen on %s: %v", port, err)
	}

	s := grpc.NewServer()

	healthcheck := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s, healthcheck)

	healthcheck.SetServingStatus("shuttle.Shuttle", grpc_health_v1.HealthCheckResponse_SERVING)
	healthcheck.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	b := bus.New()
	h := handler.New(b)
	pb.RegisterShuttleServer(s, h)

	log.Printf("server running on %s\n", port)
	if err := s.Serve(l); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
