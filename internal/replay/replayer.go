package replay

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// OutputMode controls how much detail is printed per event.
type OutputMode string

const (
	OutputFull    OutputMode = "full"
	OutputSummary OutputMode = "summary"
	OutputKeyOnly OutputMode = "key-only"
)

// Config holds replay configuration.
type Config struct {
	Speed  float64
	Output OutputMode
	Writer io.Writer
	Clock  Clock
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(w io.Writer) Config {
	return Config{
		Speed:  1.0,
		Output: OutputFull,
		Writer: w,
		Clock:  RealClock{},
	}
}

// Replayer walks through events in timestamp order with timing simulation.
type Replayer struct {
	config Config
}

// NewReplayer creates a Replayer with the given config.
func NewReplayer(config Config) Replayer {
	if config.Speed < 0 {
		config.Speed = 1.0
	}
	if config.Clock == nil {
		config.Clock = RealClock{}
	}
	if config.Output == "" {
		config.Output = OutputFull
	}
	return Replayer{config: config}
}

// Replay walks through events, sleeping between them to simulate timing.
func (r Replayer) Replay(events []model.Event) error {
	for i, event := range events {
		if i > 0 && r.config.Speed > 0 {
			delay := event.Timestamp.Sub(events[i-1].Timestamp)
			scaledDelay := time.Duration(float64(delay) / r.config.Speed)
			if scaledDelay > 0 {
				r.config.Clock.Sleep(scaledDelay)
			}
		}

		if err := r.printEvent(i+1, event); err != nil {
			return fmt.Errorf("writing event %d: %w", i+1, err)
		}
	}
	return nil
}

func (r Replayer) printEvent(seq int, event model.Event) error {
	switch r.config.Output {
	case OutputKeyOnly:
		_, err := fmt.Fprintf(r.config.Writer, "[%d] %s | %s | %s\n",
			seq, event.Timestamp.Format(time.RFC3339), event.EventType, event.Key)
		return err

	case OutputSummary:
		_, err := fmt.Fprintf(r.config.Writer, "[%d] %s | %-20s | key=%-15s | topic=%-15s | partition=%d | offset=%d\n",
			seq, event.Timestamp.Format(time.RFC3339), event.EventType, event.Key,
			event.Topic, event.Partition, event.Offset)
		return err

	default: // OutputFull
		prettyValue, err := json.MarshalIndent(json.RawMessage(event.Value), "    ", "  ")
		if err != nil {
			prettyValue = event.Value
		}
		_, err = fmt.Fprintf(r.config.Writer,
			"[%d] %s\n  Type:      %s\n  Key:       %s\n  Topic:     %s\n  Partition: %d\n  Offset:    %d\n  Headers:   %v\n  Value:\n    %s\n\n",
			seq, event.Timestamp.Format(time.RFC3339),
			event.EventType, event.Key, event.Topic,
			event.Partition, event.Offset, event.Headers, prettyValue)
		return err
	}
}
