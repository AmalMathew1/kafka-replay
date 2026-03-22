package filter

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// Apply filters events by the given criteria. Returns a new slice; never mutates input.
// Dimensions use AND semantics; values within a dimension use OR.
func Apply(events []model.Event, criteria model.FilterCriteria) []model.Event {
	if criteria.IsEmpty() {
		result := make([]model.Event, len(events))
		copy(result, events)
		return result
	}

	result := make([]model.Event, 0, len(events))
	for _, event := range events {
		if matchesAll(event, criteria) {
			result = append(result, event)
		}
	}
	return result
}

func matchesAll(event model.Event, c model.FilterCriteria) bool {
	if len(c.EventTypes) > 0 && !matchesAny(event.EventType, c.EventTypes) {
		return false
	}
	if c.TimeFrom != nil && event.Timestamp.Before(*c.TimeFrom) {
		return false
	}
	if c.TimeTo != nil && event.Timestamp.After(*c.TimeTo) {
		return false
	}
	if len(c.Keys) > 0 && !matchesAny(event.Key, c.Keys) {
		return false
	}
	for _, ff := range c.FieldFilters {
		if !matchesFieldFilter(event, ff) {
			return false
		}
	}
	return true
}

func matchesAny(value string, candidates []string) bool {
	for _, c := range candidates {
		if value == c {
			return true
		}
	}
	return false
}

func matchesFieldFilter(event model.Event, ff model.FieldFilter) bool {
	val, found := ExtractFieldString(event.Value, ff.Path)

	switch ff.Operator {
	case "exists":
		return found
	case "eq", "":
		return found && val == ff.Value
	case "contains":
		return found && strings.Contains(val, ff.Value)
	case "gt":
		return found && compareNumeric(val, ff.Value) > 0
	case "lt":
		return found && compareNumeric(val, ff.Value) < 0
	default:
		return false
	}
}

// compareNumeric compares two string values as float64.
// Returns -1, 0, or 1. Falls back to string comparison if not numeric.
func compareNumeric(a, b string) int {
	fa, errA := strconv.ParseFloat(a, 64)
	fb, errB := strconv.ParseFloat(b, 64)

	if errA != nil || errB != nil {
		return strings.Compare(a, b)
	}

	switch {
	case fa < fb:
		return -1
	case fa > fb:
		return 1
	default:
		return 0
	}
}

// Count returns the number of events matching the criteria.
func Count(events []model.Event, criteria model.FilterCriteria) int {
	if criteria.IsEmpty() {
		return len(events)
	}

	count := 0
	for _, event := range events {
		if matchesAll(event, criteria) {
			count++
		}
	}
	return count
}

// FormatFieldFilter parses a "path=value" string into a FieldFilter.
func FormatFieldFilter(s string, operator string) (model.FieldFilter, error) {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return model.FieldFilter{}, fmt.Errorf("invalid field filter %q: expected path=value", s)
	}
	if operator == "" {
		operator = "eq"
	}
	return model.FieldFilter{
		Path:     parts[0],
		Operator: operator,
		Value:    parts[1],
	}, nil
}
