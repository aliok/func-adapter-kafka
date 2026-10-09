package kafka

import (
	"context"

	"github.com/cloudevents/sdk-go/v2/event"
)

// KafkaAdapter delivers a consumed Kafka record (already converted to a
// CloudEvent) to the user function and returns the function's optional response
// event. A nil error means the record was handled successfully and its offset
// may be committed; a non-nil error means the record must be redelivered.
//
// The response event is ignored today. The interface is request/response so a
// future version can produce that event back to a Kafka topic without changing
// the contract.
type KafkaAdapter interface {
	Invoke(ctx context.Context, e event.Event) (*event.Event, error)
}
