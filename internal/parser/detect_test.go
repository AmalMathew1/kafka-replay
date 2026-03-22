package parser_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/AmalMathew1/kafka-replay/internal/parser"
)

func testdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..", "testdata")
}

func TestDetectAndParse_JSONArray(t *testing.T) {
	path := filepath.Join(testdataDir(), "events", "simple.json")
	events, err := parser.DetectAndParse(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
	if events[0].EventType != "OrderCreated" {
		t.Errorf("expected EventType 'OrderCreated', got %q", events[0].EventType)
	}
}

func TestDetectAndParse_JSONLines(t *testing.T) {
	path := filepath.Join(testdataDir(), "events", "simple.jsonl")
	events, err := parser.DetectAndParse(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
}

func TestDetectAndParse_EmptyArray(t *testing.T) {
	path := filepath.Join(testdataDir(), "events", "empty.json")
	events, err := parser.DetectAndParse(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}

func TestDetectAndParse_MalformedJSON(t *testing.T) {
	path := filepath.Join(testdataDir(), "events", "malformed.json")
	_, err := parser.DetectAndParse(path)
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestDetectAndParse_FileNotFound(t *testing.T) {
	_, err := parser.DetectAndParse("/nonexistent/file.json")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}
