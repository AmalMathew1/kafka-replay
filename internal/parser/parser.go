package parser

import (
	"io"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// EventParser parses Kafka events from a reader.
type EventParser interface {
	Parse(r io.Reader) ([]model.Event, error)
}
