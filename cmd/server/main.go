package main

import (
	"log"
	"net"
	"os"

	"github.com/joho/godotenv"
	"github.com/portbound/shuttle/internal/bus"
	"github.com/portbound/shuttle/internal/server"
	pb "github.com/portbound/shuttle/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	godotenv.Load()
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		log.Fatal("SERVER_PORT environment variable is not set")
	}
	grpcServer := grpc.NewServer()
	healthcheck := health.NewServer()

	grpc_health_v1.RegisterHealthServer(grpcServer, healthcheck)
	healthcheck.SetServingStatus("shuttle.Shuttle", grpc_health_v1.HealthCheckResponse_SERVING)

	shuttleServer := server.New(bus.New())
	pb.RegisterShuttleServer(grpcServer, shuttleServer)

	l, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("listen on %s: %v", port, err)
	}

	log.Printf("listening on %s", port)
	if err := grpcServer.Serve(l); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
