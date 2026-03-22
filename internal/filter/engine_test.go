package filter_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/filter"
	"github.com/AmalMathew1/kafka-replay/internal/model"
)

var (
	t0 = time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	t1 = time.Date(2024, 6, 15, 10, 1, 0, 0, time.UTC)
	t2 = time.Date(2024, 6, 15, 10, 5, 0, 0, time.UTC)
	t3 = time.Date(2024, 6, 15, 10, 10, 0, 0, time.UTC)
)

func testEvents() []model.Event {
	return []model.Event{
		model.NewEvent("orders", 0, 1, "order-101",
			json.RawMessage(`{"order_id":"101","amount":59.99,"status":"created"}`),
			nil, t0, "OrderCreated"),
		model.NewEvent("orders", 0, 2, "order-102",
			json.RawMessage(`{"order_id":"102","amount":149.50,"status":"created"}`),
			nil, t1, "OrderCreated"),
		model.NewEvent("orders", 1, 3, "order-101",
			json.RawMessage(`{"order_id":"101","amount":59.99,"status":"confirmed"}`),
			nil, t2, "OrderConfirmed"),
		model.NewEvent("payments", 0, 4, "pay-201",
			json.RawMessage(`{"payment_id":"201","order_id":"101","amount":59.99}`),
			nil, t3, "PaymentProcessed"),
	}
}

func TestApply_EmptyCriteria_ReturnsAll(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{})
	if len(result) != 4 {
		t.Fatalf("expected 4 events, got %d", len(result))
	}
}

func TestApply_EmptyCriteria_ReturnsCopy(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{})
	result[0] = model.Event{}
	if events[0].Key == "" {
		t.Error("original slice was mutated")
	}
}

func TestApply_ByEventType_Single(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		EventTypes: []string{"OrderCreated"},
	})
	if len(result) != 2 {
		t.Fatalf("expected 2 OrderCreated events, got %d", len(result))
	}
	for _, e := range result {
		if e.EventType != "OrderCreated" {
			t.Errorf("unexpected event type %q", e.EventType)
		}
	}
}

func TestApply_ByEventType_Multiple_OR(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		EventTypes: []string{"OrderCreated", "PaymentProcessed"},
	})
	if len(result) != 3 {
		t.Fatalf("expected 3 events, got %d", len(result))
	}
}

func TestApply_ByKey(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		Keys: []string{"order-101"},
	})
	if len(result) != 2 {
		t.Fatalf("expected 2 events for order-101, got %d", len(result))
	}
}

func TestApply_ByTimeRange(t *testing.T) {
	events := testEvents()
	from := t1
	to := t2
	result := filter.Apply(events, model.FilterCriteria{
		TimeFrom: &from,
		TimeTo:   &to,
	})
	if len(result) != 2 {
		t.Fatalf("expected 2 events in time range, got %d", len(result))
	}
}

func TestApply_ByTimeFrom_Only(t *testing.T) {
	events := testEvents()
	from := t2
	result := filter.Apply(events, model.FilterCriteria{
		TimeFrom: &from,
	})
	if len(result) != 2 {
		t.Fatalf("expected 2 events from t2 onward, got %d", len(result))
	}
}

func TestApply_ByTimeTo_Only(t *testing.T) {
	events := testEvents()
	to := t1
	result := filter.Apply(events, model.FilterCriteria{
		TimeTo: &to,
	})
	if len(result) != 2 {
		t.Fatalf("expected 2 events up to t1, got %d", len(result))
	}
}

func TestApply_CombinedFilters_AND(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		EventTypes: []string{"OrderCreated"},
		Keys:       []string{"order-101"},
	})
	if len(result) != 1 {
		t.Fatalf("expected 1 event (OrderCreated AND order-101), got %d", len(result))
	}
	if result[0].Key != "order-101" || result[0].EventType != "OrderCreated" {
		t.Errorf("unexpected event: %+v", result[0])
	}
}

func TestApply_NoMatches(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		EventTypes: []string{"NonExistent"},
	})
	if len(result) != 0 {
		t.Fatalf("expected 0 events, got %d", len(result))
	}
}

func TestApply_FieldFilter_Eq(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		FieldFilters: []model.FieldFilter{
			{Path: "status", Operator: "eq", Value: "confirmed"},
		},
	})
	if len(result) != 1 {
		t.Fatalf("expected 1 event with status=confirmed, got %d", len(result))
	}
	if result[0].EventType != "OrderConfirmed" {
		t.Errorf("expected OrderConfirmed, got %q", result[0].EventType)
	}
}

func TestApply_FieldFilter_Contains(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		FieldFilters: []model.FieldFilter{
			{Path: "order_id", Operator: "contains", Value: "102"},
		},
	})
	if len(result) != 1 {
		t.Fatalf("expected 1 event with order_id containing '102', got %d", len(result))
	}
	if result[0].Key != "order-102" {
		t.Errorf("expected order-102, got %q", result[0].Key)
	}
}

func TestApply_FieldFilter_Gt(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		FieldFilters: []model.FieldFilter{
			{Path: "amount", Operator: "gt", Value: "100"},
		},
	})
	if len(result) != 1 {
		t.Fatalf("expected 1 event with amount > 100, got %d", len(result))
	}
	if result[0].Key != "order-102" {
		t.Errorf("expected order-102, got %q", result[0].Key)
	}
}

func TestApply_FieldFilter_Lt(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		FieldFilters: []model.FieldFilter{
			{Path: "amount", Operator: "lt", Value: "100"},
		},
	})
	if len(result) != 3 {
		t.Fatalf("expected 3 events with amount < 100, got %d", len(result))
	}
}

func TestApply_FieldFilter_Exists(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		FieldFilters: []model.FieldFilter{
			{Path: "payment_id", Operator: "exists"},
		},
	})
	if len(result) != 1 {
		t.Fatalf("expected 1 event with payment_id, got %d", len(result))
	}
	if result[0].EventType != "PaymentProcessed" {
		t.Errorf("expected PaymentProcessed, got %q", result[0].EventType)
	}
}

func TestApply_MultipleFieldFilters_AND(t *testing.T) {
	events := testEvents()
	result := filter.Apply(events, model.FilterCriteria{
		FieldFilters: []model.FieldFilter{
			{Path: "order_id", Operator: "eq", Value: "101"},
			{Path: "status", Operator: "eq", Value: "created"},
		},
	})
	if len(result) != 1 {
		t.Fatalf("expected 1 event matching both field filters, got %d", len(result))
	}
}

func TestCount_MatchesCriteria(t *testing.T) {
	events := testEvents()
	count := filter.Count(events, model.FilterCriteria{
		EventTypes: []string{"OrderCreated"},
	})
	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}
}

func TestCount_EmptyCriteria(t *testing.T) {
	events := testEvents()
	count := filter.Count(events, model.FilterCriteria{})
	if count != 4 {
		t.Errorf("expected count 4, got %d", count)
	}
}

func TestFormatFieldFilter_Valid(t *testing.T) {
	ff, err := filter.FormatFieldFilter("status=created", "eq")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ff.Path != "status" || ff.Value != "created" || ff.Operator != "eq" {
		t.Errorf("unexpected filter: %+v", ff)
	}
}

func TestFormatFieldFilter_DefaultOperator(t *testing.T) {
	ff, err := filter.FormatFieldFilter("status=created", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ff.Operator != "eq" {
		t.Errorf("expected default operator 'eq', got %q", ff.Operator)
	}
}

func TestFormatFieldFilter_Invalid(t *testing.T) {
	_, err := filter.FormatFieldFilter("no-equals-sign", "eq")
	if err == nil {
		t.Fatal("expected error for invalid format, got nil")
	}
}

func TestFormatFieldFilter_ValueWithEquals(t *testing.T) {
	ff, err := filter.FormatFieldFilter("url=https://example.com?a=b", "eq")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ff.Value != "https://example.com?a=b" {
		t.Errorf("expected full value after first =, got %q", ff.Value)
	}
}
