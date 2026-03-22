package model

import "time"

// FilterCriteria defines criteria for filtering events.
// Dimensions use AND semantics; values within a dimension use OR.
type FilterCriteria struct {
	EventTypes   []string
	TimeFrom     *time.Time
	TimeTo       *time.Time
	Keys         []string
	FieldFilters []FieldFilter
}

// FieldFilter matches a JSON field path against a value using an operator.
type FieldFilter struct {
	Path     string // Dot-notation path, e.g. "payload.orderId"
	Operator string // "eq", "contains", "gt", "lt", "exists"
	Value    string // String representation; compared after type coercion
}

// IsEmpty returns true if no filter criteria are set.
func (f FilterCriteria) IsEmpty() bool {
	return len(f.EventTypes) == 0 &&
		f.TimeFrom == nil &&
		f.TimeTo == nil &&
		len(f.Keys) == 0 &&
		len(f.FieldFilters) == 0
}
