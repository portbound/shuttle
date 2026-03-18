package main

import (
	"log"
	"net"

	"github.com/portbound/shuttle/internal/grpc"
	pb "github.com/portbound/shuttle/proto"
	"google.golang.org/grpc"
)

const port string = ":8080"

func main() {
	l, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("listen on %s: %v", port, err)
	}

	s := grpc.NewServer()
	pb.RegisterShuttleServer(s, &server.Handler{})
	log.Printf("server running on %s\n", port)

	if err := s.Serve(l); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
