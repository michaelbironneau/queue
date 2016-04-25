// Package queue provides a wrapper to create worker applications from cloud-based queuing services such as Azure Service Bus.
package queue

//Item is a generic item in a queue.
type Item struct {
	ID        string //Unique item ID, assigned by the queuing service.
	LockToken string //Lock token held by the worker while it processing the message, assigned by the queueing service (called Receipt Handle in AWS).
	Request   []byte //Request payload.
}

//Queue is a request queue for worker processes. A worker gets the Next() item in the queue, does some work based on that item, and either calls
//Succeed() or Fail() depending on the outcome. Note that Fail() returns the item to the queue.
type Queue interface {
	Next() (*Item, error)
	Succeed(*Item) error
	Fail(*Item) error
}
