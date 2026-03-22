package eventstore

import (
	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// EventStore provides access to loaded Kafka events.
type EventStore interface {
	// Load parses events from one or more files into the store.
	Load(filePaths ...string) error

	// All returns all events sorted by timestamp.
	All() []model.Event

	// Count returns the total number of loaded events.
	Count() int
}
