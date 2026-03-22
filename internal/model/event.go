package model

import (
	"encoding/json"
	"time"
)

// Event represents a single Kafka event record loaded from an export file.
// All fields are immutable after construction via NewEvent.
type Event struct {
	Topic     string            `json:"topic"`
	Partition int               `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     json.RawMessage   `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	EventType string            `json:"event_type"`
}

// NewEvent creates a new Event with all fields set.
func NewEvent(
	topic string,
	partition int,
	offset int64,
	key string,
	value json.RawMessage,
	headers map[string]string,
	timestamp time.Time,
	eventType string,
) Event {
	headersCopy := make(map[string]string, len(headers))
	for k, v := range headers {
		headersCopy[k] = v
	}

	valueCopy := make(json.RawMessage, len(value))
	copy(valueCopy, value)

	return Event{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       key,
		Value:     valueCopy,
		Headers:   headersCopy,
		Timestamp: timestamp,
		EventType: eventType,
	}
}

// WithEventType returns a new Event with the event type set.
func (e Event) WithEventType(eventType string) Event {
	return NewEvent(e.Topic, e.Partition, e.Offset, e.Key, e.Value, e.Headers, e.Timestamp, eventType)
}
