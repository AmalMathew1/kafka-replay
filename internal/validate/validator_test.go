package validate_test

import (
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/model"
	"github.com/AmalMathew1/kafka-replay/internal/validate"
)

func testdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..", "testdata")
}

var t0 = time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)

func orderEvent(id string, amount float64, status string) model.Event {
	val := map[string]any{"order_id": id, "amount": amount, "status": status}
	raw, _ := json.Marshal(val)
	return model.NewEvent("orders", 0, 1, "key-"+id, raw, nil, t0, "OrderCreated")
}

func TestValidate_AllValid(t *testing.T) {
	events := []model.Event{
		orderEvent("1", 50.0, "created"),
		orderEvent("2", 100.0, "confirmed"),
	}
	schemaPath := filepath.Join(testdataDir(), "schemas", "order_event.schema.json")

	result, err := validate.ValidateEvents(events, schemaPath, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.ValidCount != 2 {
		t.Errorf("expected 2 valid, got %d", result.ValidCount)
	}
	if result.InvalidCount != 0 {
		t.Errorf("expected 0 invalid, got %d", result.InvalidCount)
	}
}

func TestValidate_InvalidStatus(t *testing.T) {
	events := []model.Event{
		orderEvent("1", 50.0, "created"),
		orderEvent("2", 100.0, "invalid_status"),
	}
	schemaPath := filepath.Join(testdataDir(), "schemas", "order_event.schema.json")

	result, err := validate.ValidateEvents(events, schemaPath, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.ValidCount != 1 {
		t.Errorf("expected 1 valid, got %d", result.ValidCount)
	}
	if result.InvalidCount != 1 {
		t.Errorf("expected 1 invalid, got %d", result.InvalidCount)
	}
	if len(result.Errors) != 1 {
		t.Fatalf("expected 1 error entry, got %d", len(result.Errors))
	}
	if result.Errors[0].Index != 1 {
		t.Errorf("expected error at index 1, got %d", result.Errors[0].Index)
	}
}

func TestValidate_MissingRequiredField(t *testing.T) {
	raw := json.RawMessage(`{"order_id": "1"}`)
	event := model.NewEvent("orders", 0, 1, "key-1", raw, nil, t0, "OrderCreated")
	schemaPath := filepath.Join(testdataDir(), "schemas", "order_event.schema.json")

	result, err := validate.ValidateEvents([]model.Event{event}, schemaPath, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.InvalidCount != 1 {
		t.Errorf("expected 1 invalid, got %d", result.InvalidCount)
	}
}

func TestValidate_NegativeAmount(t *testing.T) {
	events := []model.Event{orderEvent("1", -10.0, "created")}
	schemaPath := filepath.Join(testdataDir(), "schemas", "order_event.schema.json")

	result, err := validate.ValidateEvents(events, schemaPath, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.InvalidCount != 1 {
		t.Errorf("expected 1 invalid for negative amount, got %d", result.InvalidCount)
	}
}

func TestValidate_StrictMode_StopsAtFirst(t *testing.T) {
	events := []model.Event{
		orderEvent("1", 50.0, "bad_status"),
		orderEvent("2", 100.0, "also_bad"),
	}
	schemaPath := filepath.Join(testdataDir(), "schemas", "order_event.schema.json")

	result, err := validate.ValidateEvents(events, schemaPath, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Errors) != 1 {
		t.Errorf("strict mode should stop at first error, got %d errors", len(result.Errors))
	}
}

func TestValidate_InvalidSchemaFile(t *testing.T) {
	events := []model.Event{orderEvent("1", 50.0, "created")}
	schemaPath := filepath.Join(testdataDir(), "schemas", "invalid.schema.json")

	_, err := validate.ValidateEvents(events, schemaPath, false)
	if err == nil {
		t.Fatal("expected error for invalid schema, got nil")
	}
}

func TestValidate_SchemaFileNotFound(t *testing.T) {
	events := []model.Event{orderEvent("1", 50.0, "created")}

	_, err := validate.ValidateEvents(events, "/nonexistent/schema.json", false)
	if err == nil {
		t.Fatal("expected error for missing schema, got nil")
	}
}

func TestValidate_EmptyEvents(t *testing.T) {
	schemaPath := filepath.Join(testdataDir(), "schemas", "order_event.schema.json")

	result, err := validate.ValidateEvents(nil, schemaPath, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalEvents != 0 {
		t.Errorf("expected 0 total, got %d", result.TotalEvents)
	}
}

func TestValidate_AdditionalProperties(t *testing.T) {
	raw := json.RawMessage(`{"order_id":"1","amount":50,"status":"created","extra_field":"nope"}`)
	event := model.NewEvent("orders", 0, 1, "key-1", raw, nil, t0, "OrderCreated")
	schemaPath := filepath.Join(testdataDir(), "schemas", "order_event.schema.json")

	result, err := validate.ValidateEvents([]model.Event{event}, schemaPath, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.InvalidCount != 1 {
		t.Errorf("expected 1 invalid for extra properties, got %d", result.InvalidCount)
	}
}

func TestValidate_PaymentSchema(t *testing.T) {
	raw := json.RawMessage(`{"payment_id":"p1","order_id":"o1","amount":50,"method":"credit_card"}`)
	event := model.NewEvent("payments", 0, 1, "key-1", raw, nil, t0, "PaymentProcessed")
	schemaPath := filepath.Join(testdataDir(), "schemas", "payment_event.schema.json")

	result, err := validate.ValidateEvents([]model.Event{event}, schemaPath, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.ValidCount != 1 {
		t.Errorf("expected 1 valid, got %d", result.ValidCount)
	}
}
