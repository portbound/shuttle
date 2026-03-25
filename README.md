# Shuttle

Shuttle is a high-performance message bus implemented in Go using gRPC. It provides a simple API for publishing and subscribing to messages with support for consumer groups and health monitoring.

## Features

- **gRPC Native**: Built on gRPC for efficient, cross-language communication.
- **Consumer Groups**: Distributed message processing across multiple subscribers.
- **Health Monitoring**: Integrated gRPC health checking for service reliability.
- **Simple Client**: A clean Go client library for easy integration.

## Installation

```bash
go get github.com/portbound/shuttle
```

## Usage

### Server

Start the Shuttle server:

```bash
go run cmd/server/main.go
```

### Client

#### Initialize Client

```go
import "github.com/portbound/shuttle/pkg/shuttle"

sh, err := shuttle.New("localhost:50051")
if err != nil {
    log.Fatal(err)
}
defer sh.Close()
```

#### Publish

```go
msgId, err := sh.Publish(ctx, "updates", []byte("payload"))
```

#### Subscribe

Subscribing with a group name enables load balancing across consumers in that group.

```go
ch, err := sh.Subscribe(ctx, "updates", "worker-group")
if err != nil {
    log.Fatal(err)
}

for msg := range ch {
    fmt.Printf("Received %s: %s\n", msg.MessageId, string(msg.Payload))
}
```

## API Reference

### `pkg/shuttle`

| Method | Description |
|--------|-------------|
| `New(addr string, opts ...Option)` | Creates a new Shuttle client. |
| `Publish(ctx, topic, data)` | Publishes a message to a topic. |
| `Subscribe(ctx, topic, group)` | Subscribes to a topic within a consumer group. |
| `ListTopics(ctx)` | Returns a list of all active topics. |
| `CheckHealth(ctx)` | Returns the current serving status of the server. |
| `WatchHealth(ctx)` | Returns a channel for streaming health updates. |
| `Close()` | Closes the underlying gRPC connection. |

## Example

For a complete implementation demonstrating concurrent publishers and subscribers, refer to `example/main.go`.

```go
// Run the example
go run example/main.go
```

## Protocol

The service is defined in `proto/shuttle.proto`:

```proto
service Shuttle {
  rpc Publish(PublishRequest) returns (PublishResponse);
  rpc Subscribe(SubscribeRequest) returns (stream SubscribeResponse);
  rpc ListTopics(ListTopicsRequest) returns (ListTopicsResponse);
}
```
