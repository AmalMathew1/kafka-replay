package eventstore

import (
	"fmt"
	"sort"

	"github.com/AmalMathew1/kafka-replay/internal/model"
	"github.com/AmalMathew1/kafka-replay/internal/parser"
)

// MemoryStore is an in-memory EventStore that sorts events by timestamp.
type MemoryStore struct {
	events []model.Event
}

// NewMemoryStore creates a new empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		events: make([]model.Event, 0),
	}
}

func (s *MemoryStore) Load(filePaths ...string) error {
	var allEvents []model.Event

	for _, path := range filePaths {
		events, err := parser.DetectAndParse(path)
		if err != nil {
			return fmt.Errorf("loading %s: %w", path, err)
		}
		allEvents = append(allEvents, events...)
	}

	sort.Slice(allEvents, func(i, j int) bool {
		return allEvents[i].Timestamp.Before(allEvents[j].Timestamp)
	})

	s.events = allEvents
	return nil
}

func (s *MemoryStore) All() []model.Event {
	result := make([]model.Event, len(s.events))
	copy(result, s.events)
	return result
}

func (s *MemoryStore) Count() int {
	return len(s.events)
}
