# Cloud Queue wrapper

This implements a Queue interface backed by queue services like Azure Service Bus queues or Amazon SQS. It looks like this:

```go
//Queue is a request queue for worker processes. A worker Assign()s itself an item, does some work based on that item, and either calls
//Succeed() or Fail() depending on the outcome. Note that Fail() returns the item to the queue.
//Ping() can be used to verify connectivity to the queue (check implementation details).
type Queue interface {
	Next() (*Item, error)
	Succeed(*Item) error
	Fail(*Item) error
}

```