package parser_test

import (
	"strings"
	"testing"

	"github.com/AmalMathew1/kafka-replay/internal/parser"
)

func TestJSONArrayParser_ParsesValidArray(t *testing.T) {
	input := `[
		{"topic":"orders","partition":0,"offset":1,"key":"k1","value":{"a":1},"timestamp":"2024-06-15T10:00:00Z","event_type":"OrderCreated"},
		{"topic":"orders","partition":0,"offset":2,"key":"k2","value":{"a":2},"timestamp":"2024-06-15T10:01:00Z","event_type":"OrderCreated"}
	]`

	p := parser.JSONArrayParser{}
	events, err := p.Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Key != "k1" {
		t.Errorf("expected key 'k1', got %q", events[0].Key)
	}
	if events[1].Offset != 2 {
		t.Errorf("expected offset 2, got %d", events[1].Offset)
	}
}

func TestJSONArrayParser_EmptyArray(t *testing.T) {
	p := parser.JSONArrayParser{}
	events, err := p.Parse(strings.NewReader("[]"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}

func TestJSONArrayParser_MalformedInput(t *testing.T) {
	p := parser.JSONArrayParser{}
	_, err := p.Parse(strings.NewReader(`[{"topic": INVALID}]`))
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestJSONLinesParser_ParsesValidLines(t *testing.T) {
	input := `{"topic":"orders","partition":0,"offset":1,"key":"k1","value":{"a":1},"timestamp":"2024-06-15T10:00:00Z","event_type":"OrderCreated"}
{"topic":"orders","partition":0,"offset":2,"key":"k2","value":{"a":2},"timestamp":"2024-06-15T10:01:00Z","event_type":"OrderCreated"}`

	p := parser.JSONLinesParser{}
	events, err := p.Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestJSONLinesParser_SkipsBlankLines(t *testing.T) {
	input := `{"topic":"orders","partition":0,"offset":1,"key":"k1","value":{},"timestamp":"2024-06-15T10:00:00Z","event_type":"A"}

{"topic":"orders","partition":0,"offset":2,"key":"k2","value":{},"timestamp":"2024-06-15T10:01:00Z","event_type":"B"}
`

	p := parser.JSONLinesParser{}
	events, err := p.Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestJSONLinesParser_MalformedLine(t *testing.T) {
	input := `{"topic":"orders","partition":0,"offset":1,"key":"k1","value":{},"timestamp":"2024-06-15T10:00:00Z","event_type":"A"}
NOT VALID JSON`

	p := parser.JSONLinesParser{}
	_, err := p.Parse(strings.NewReader(input))
	if err == nil {
		t.Fatal("expected error for malformed line, got nil")
	}
}
