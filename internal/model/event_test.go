package model_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

func TestNewEvent_CopiesHeaders(t *testing.T) {
	headers := map[string]string{"key": "value"}
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	value := json.RawMessage(`{"amount": 100}`)

	event := model.NewEvent("orders", 0, 1, "order-1", value, headers, ts, "OrderCreated")

	// Mutating the original map should not affect the event.
	headers["key"] = "mutated"
	if event.Headers["key"] != "value" {
		t.Errorf("expected header 'value', got %q", event.Headers["key"])
	}
}

func TestNewEvent_CopiesValue(t *testing.T) {
	value := json.RawMessage(`{"amount": 100}`)
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	event := model.NewEvent("orders", 0, 1, "order-1", value, nil, ts, "OrderCreated")

	// Mutating the original slice should not affect the event.
	value[0] = 'X'
	if event.Value[0] == 'X' {
		t.Error("expected Value to be a copy, but mutation was visible")
	}
}

func TestWithEventType_ReturnsNewEvent(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	value := json.RawMessage(`{"amount": 100}`)

	original := model.NewEvent("orders", 0, 1, "order-1", value, nil, ts, "OrderCreated")
	updated := original.WithEventType("OrderUpdated")

	if original.EventType != "OrderCreated" {
		t.Errorf("original should be unchanged, got %q", original.EventType)
	}
	if updated.EventType != "OrderUpdated" {
		t.Errorf("updated should have new type, got %q", updated.EventType)
	}
	if updated.Key != original.Key {
		t.Errorf("updated should preserve key, got %q", updated.Key)
	}
}
