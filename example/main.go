package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/portbound/shuttle/pkg/shuttle"
)

var (
	// you can trigger the gRPC DNS Resolver by prefixing your address with dns:///
	// if you're using K8s, this will enable client-side load balancing by allowing a single client to discover and connect to all pods
	// e.g. target = "shuttle-svc.namespace.svc.cluster.local:50051"

	// it can also be used to connect to a specific domain if you have the service running in a container behind a reverse proxy
	// e.g. target = "shuttle.domain.com"

	target = ":50051"
	addr   = fmt.Sprintf("dns:///%s", target)
	topic  = "test"
)

func main() {
	ctx := context.Background()
	var wg sync.WaitGroup

	wg.Go(func() {
		watchHealth(ctx)
	})
	wg.Go(func() {
		subscribe(ctx, 1, "test-group")
	})
	wg.Go(func() {
		subscribe(ctx, 2, "test-group")
	})
	wg.Go(func() {
		subscribe(ctx, 3, "test-group")
	})
	wg.Go(func() {
		go publish(ctx)
	})
	wg.Wait()
}

func publish(ctx context.Context) {
	sh, err := shuttle.New(addr, shuttle.WithInsecure())
	if err != nil {
		log.Fatalf("new shuttle: %v", err)
	}
	defer sh.Close()

	ticker := time.NewTicker(time.Millisecond * 5)
	defer ticker.Stop()

	for range ticker.C {
		time := time.Now().String()
		msgId, err := sh.Publish(ctx, topic, []byte(time))
		if err != nil {
			log.Printf("publish: %v", err)
		}
		fmt.Printf("published:\n%s\n%s\n\n", msgId, time)
	}
}

func subscribe(ctx context.Context, client int, group string) {
	sh, err := shuttle.New(addr, shuttle.WithInsecure())
	if err != nil {
		log.Fatalf("new shuttle: %v", err)
	}
	defer sh.Close()

	ch, err := sh.Subscribe(ctx, topic, group)
	if err != nil {
		log.Printf("subscribe: %v", err)
	}

	for msg := range ch {
		fmt.Printf("Client %d received:\n%s\n%s\n\n", client, msg.MessageId, msg.Payload)
	}
}

func watchHealth(ctx context.Context) {
	sh, err := shuttle.New(addr, shuttle.WithInsecure())
	if err != nil {
		log.Fatalf("new shuttle: %v", err)
	}
	defer sh.Close()

	ch, err := sh.WatchHealth(ctx)
	if err != nil {
		log.Printf("watch health: %v", err)
	}

	for healthCheck := range ch {
		fmt.Printf("Health: %v\n\n", healthCheck)
	}
}
