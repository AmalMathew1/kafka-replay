package filter_test

import (
	"encoding/json"
	"testing"

	"github.com/AmalMathew1/kafka-replay/internal/filter"
)

func TestExtractField_TopLevel(t *testing.T) {
	raw := json.RawMessage(`{"name": "Alice", "age": 30}`)

	val, ok := filter.ExtractField(raw, "name")
	if !ok {
		t.Fatal("expected field to be found")
	}
	if val != "Alice" {
		t.Errorf("expected 'Alice', got %v", val)
	}
}

func TestExtractField_Nested(t *testing.T) {
	raw := json.RawMessage(`{"order": {"id": "123", "item": {"sku": "ABC"}}}`)

	val, ok := filter.ExtractField(raw, "order.item.sku")
	if !ok {
		t.Fatal("expected nested field to be found")
	}
	if val != "ABC" {
		t.Errorf("expected 'ABC', got %v", val)
	}
}

func TestExtractField_NumericValue(t *testing.T) {
	raw := json.RawMessage(`{"amount": 59.99}`)

	val, ok := filter.ExtractField(raw, "amount")
	if !ok {
		t.Fatal("expected field to be found")
	}
	f, ok := val.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", val)
	}
	if f != 59.99 {
		t.Errorf("expected 59.99, got %v", f)
	}
}

func TestExtractField_BoolValue(t *testing.T) {
	raw := json.RawMessage(`{"active": true}`)

	val, ok := filter.ExtractField(raw, "active")
	if !ok {
		t.Fatal("expected field to be found")
	}
	if val != true {
		t.Errorf("expected true, got %v", val)
	}
}

func TestExtractField_NullValue(t *testing.T) {
	raw := json.RawMessage(`{"data": null}`)

	val, ok := filter.ExtractField(raw, "data")
	if !ok {
		t.Fatal("expected field to be found (null is a valid value)")
	}
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

func TestExtractField_MissingField(t *testing.T) {
	raw := json.RawMessage(`{"name": "Alice"}`)

	_, ok := filter.ExtractField(raw, "missing")
	if ok {
		t.Error("expected field to not be found")
	}
}

func TestExtractField_MissingNestedField(t *testing.T) {
	raw := json.RawMessage(`{"order": {"id": "123"}}`)

	_, ok := filter.ExtractField(raw, "order.missing.deep")
	if ok {
		t.Error("expected nested missing field to not be found")
	}
}

func TestExtractField_NonObjectIntermediate(t *testing.T) {
	raw := json.RawMessage(`{"name": "Alice"}`)

	_, ok := filter.ExtractField(raw, "name.child")
	if ok {
		t.Error("expected traversal through non-object to fail")
	}
}

func TestExtractField_InvalidJSON(t *testing.T) {
	raw := json.RawMessage(`NOT JSON`)

	_, ok := filter.ExtractField(raw, "field")
	if ok {
		t.Error("expected invalid JSON to return not found")
	}
}

func TestExtractFieldString_FormatsValues(t *testing.T) {
	raw := json.RawMessage(`{"count": 42, "name": "test"}`)

	s, ok := filter.ExtractFieldString(raw, "count")
	if !ok {
		t.Fatal("expected field to be found")
	}
	if s != "42" {
		t.Errorf("expected '42', got %q", s)
	}

	s, ok = filter.ExtractFieldString(raw, "name")
	if !ok {
		t.Fatal("expected field to be found")
	}
	if s != "test" {
		t.Errorf("expected 'test', got %q", s)
	}
}
