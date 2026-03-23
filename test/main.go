package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/portbound/shuttle/pkg/shuttle"
)

var (
	target = "localhost:50051"
	addr   = fmt.Sprintf("dns:///%s", target)
	topic  = "test"
)

func main() {

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Go(func() {
		subscribe(ctx)
	})
	wg.Go(func() {
		go publish(ctx)
	})
	wg.Wait()
}

func publish(ctx context.Context) {
	sh, err := shuttle.New(addr)
	if err != nil {
		log.Fatalf("new shuttle: %v", err)
	}
	defer sh.Close()

	msgId, err := sh.Publish(ctx, topic, []byte("hi mom!"))
	if err != nil {
		log.Fatalf("publish: %v", err)
	}

	fmt.Printf("published: %s\n", msgId)
}

func subscribe(ctx context.Context) {
	sh, err := shuttle.New(addr)
	if err != nil {
		log.Fatalf("new shuttle: %v", err)
	}
	defer sh.Close()

	ch, err := sh.Subscribe(ctx, topic, "1")
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	fmt.Println("here")
	for e := range ch {
		fmt.Printf("received: %s\n", e.Payload)
	}
}
