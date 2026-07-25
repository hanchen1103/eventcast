# eventcast

Type-safe in-process event broadcasting for Go.

`eventcast` is a small generic library for fan-out event delivery inside a
single Go process. It gives each subscriber its own buffer and an explicit
delivery policy, so slow consumers can be handled deliberately.

## Scope

`eventcast` is intentionally not a message queue:

- No persistence.
- No replay.
- No consumer groups.
- No network transport.
- No cross-process delivery.

It is meant for local component coordination, such as market-data fanout,
cache invalidation, live dashboards, agents, plugins, and in-process pipelines.

## Example

```go
package main

import (
	"context"
	"fmt"

	"github.com/hanchen1103/eventcast"
)

type Event struct {
	Message string
}

func main() {
	b := eventcast.New[Event]()
	defer b.Close()

	sub, err := b.Subscribe(
		eventcast.WithBuffer(16),
		eventcast.WithPolicy(eventcast.DropOldest),
	)
	if err != nil {
		panic(err)
	}
	defer sub.Close()

	if err := b.Publish(context.Background(), Event{Message: "hello"}); err != nil {
		panic(err)
	}

	env := <-sub.C()
	fmt.Println(env.Seq, env.Event.Message)
}
```

## Delivery Policies

- `Block`: wait until the subscriber can receive the event.
- `DropLatest`: drop the new event when the subscriber buffer is full.
- `DropOldest`: drop one old buffered event to make room for the new event.
