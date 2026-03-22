package eventstore_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/AmalMathew1/kafka-replay/internal/eventstore"
)

func testdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..", "testdata")
}

func TestMemoryStore_Load_JSON(t *testing.T) {
	store := eventstore.NewMemoryStore()
	path := filepath.Join(testdataDir(), "events", "simple.json")

	if err := store.Load(path); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if store.Count() != 5 {
		t.Fatalf("expected 5 events, got %d", store.Count())
	}
}

func TestMemoryStore_Load_JSONL(t *testing.T) {
	store := eventstore.NewMemoryStore()
	path := filepath.Join(testdataDir(), "events", "simple.jsonl")

	if err := store.Load(path); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if store.Count() != 5 {
		t.Fatalf("expected 5 events, got %d", store.Count())
	}
}

func TestMemoryStore_All_SortedByTimestamp(t *testing.T) {
	store := eventstore.NewMemoryStore()
	path := filepath.Join(testdataDir(), "events", "simple.json")

	if err := store.Load(path); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := store.All()
	for i := 1; i < len(events); i++ {
		if events[i].Timestamp.Before(events[i-1].Timestamp) {
			t.Errorf("events not sorted: event %d (%v) is before event %d (%v)",
				i, events[i].Timestamp, i-1, events[i-1].Timestamp)
		}
	}
}

func TestMemoryStore_All_ReturnsCopy(t *testing.T) {
	store := eventstore.NewMemoryStore()
	path := filepath.Join(testdataDir(), "events", "simple.json")

	if err := store.Load(path); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := store.All()
	originalCount := store.Count()

	// Mutating the returned slice should not affect the store.
	events = events[:1]
	if store.Count() != originalCount {
		t.Errorf("store count changed after mutating returned slice: got %d, want %d",
			store.Count(), originalCount)
	}
}

func TestMemoryStore_Load_MultipleFiles(t *testing.T) {
	store := eventstore.NewMemoryStore()
	jsonPath := filepath.Join(testdataDir(), "events", "simple.json")
	jsonlPath := filepath.Join(testdataDir(), "events", "simple.jsonl")

	if err := store.Load(jsonPath, jsonlPath); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if store.Count() != 10 {
		t.Fatalf("expected 10 events from 2 files, got %d", store.Count())
	}
}

func TestMemoryStore_Load_EmptyFile(t *testing.T) {
	store := eventstore.NewMemoryStore()
	path := filepath.Join(testdataDir(), "events", "empty.json")

	if err := store.Load(path); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if store.Count() != 0 {
		t.Fatalf("expected 0 events, got %d", store.Count())
	}
}

func TestMemoryStore_Load_FileNotFound(t *testing.T) {
	store := eventstore.NewMemoryStore()
	err := store.Load("/nonexistent/file.json")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestMemoryStore_Load_MalformedFile(t *testing.T) {
	store := eventstore.NewMemoryStore()
	path := filepath.Join(testdataDir(), "events", "malformed.json")
	err := store.Load(path)
	if err == nil {
		t.Fatal("expected error for malformed file, got nil")
	}
}
