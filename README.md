# Shuttle

Shuttle is a lightweight event bus implemented in Go using gRPC. 

## Features

- **Load Balancing Consumer Groups**: Distributed message processing across group members.
- **Secure Communication**: Optional TLS support.  
- **Health Monitoring**: Integrated gRPC health checking for service reliability.
- **Client SDK**: A Go client library for simple integration.

## Installation

```bash
go get github.com/portbound/shuttle
```

## Usage


### Quick Start
#### Server

Start the Shuttle server:

```bash
go run cmd/server/main.go
```

#### Client

Initialize the Shuttle client:

```go
import "github.com/portbound/shuttle/pkg/shuttle"

target = "shuttle-svc.namespace.svc.cluster.local:50051"
addr   = fmt.Sprintf("dns:///%s", target)

sh, err := shuttle.New(addr)
if err != nil {
    log.Fatal(err)
}
defer sh.Close()
```

##### Publish

```go
msgId, err := sh.Publish(ctx, "updates", []byte("payload"))
if err != nil {
    log.Fatal(err)
}
```

##### Subscribe

```go
ch, err := sh.Subscribe(ctx, "updates", "worker-group")
if err != nil {
    log.Fatal(err)
}

for msg := range ch {
    fmt.Printf("Received %s: %s\n", msg.MessageId, string(msg.Payload))
}
```

## Client SDK

### `pkg/shuttle`

### API Reference


| Method | Description |
|--------|-------------|
| `New(addr string, opts ...Option)` | Creates a new Shuttle client. |
| `Publish(ctx context.Context, topic, data string)` | Publishes a message to a topic. |
| `Subscribe(ctx, topic, group)` | Subscribes to a topic within a consumer group. |
| `ListTopics(ctx)` | Returns a list of all active topics. |
| `CheckHealth(ctx)` | Returns the current serving status of the server. |
| `WatchHealth(ctx)` | Returns a channel for streaming health updates. |
| `Close()` | Closes the underlying gRPC connection. |


### Example

For a complete implementation demonstrating concurrent publishers and subscribers, refer to `example/main.go`.

### Error Handling 
- `ErrEmptyTopic`: Returned when a Publisher or Subscriber has not provided a topic 
- `ErrPayloadTooLarge`: Returned when a Publisher attempts to push a payload that exceeds MaxPayloadSize (256kb)
- `ErrGroupBusy`: Returned when consumers are fully saturated"

## License 
This project is licensed under the MIT License - see the [LICENSE](https://github.com/portbound/shuttle/blob/main/LICENSE) file for details.
