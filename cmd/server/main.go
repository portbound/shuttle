package main

import (
	"fmt"
	"log"
	"net"

	"github.com/portbound/shuttle/internal/bus"
	"github.com/portbound/shuttle/internal/server"
	pb "github.com/portbound/shuttle/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const port string = ":50051"

func main() {
	// TODO: use TLS

	// creds, err := credentials.NewServerTLSFromFile("", "")
	// if err != nil {
	// 	log.Fatalf("get credentials: %v", err)
	// }
	// s := grpc.NewServer(grpc.Creds(creds))

	grpcServer := grpc.NewServer()

	healthcheck := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthcheck)

	healthcheck.SetServingStatus("shuttle.Shuttle", grpc_health_v1.HealthCheckResponse_SERVING)
	healthcheck.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	b := bus.New()
	shuttleServer := server.New(b)
	pb.RegisterShuttleServer(grpcServer, shuttleServer)

	l, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("listen on %s: %v", port, err)
	}

	fmt.Printf("listening on %s", port)
	if err := grpcServer.Serve(l); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
