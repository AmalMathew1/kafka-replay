package dlq_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/AmalMathew1/kafka-replay/internal/dlq"
)

func testdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..", "testdata")
}

func TestLoadDLQEntries_Valid(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_sample.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 8 {
		t.Fatalf("expected 8 entries, got %d", len(entries))
	}
	if entries[0].FailureReason != "schema_validation_failed" {
		t.Errorf("expected first reason 'schema_validation_failed', got %q", entries[0].FailureReason)
	}
}

func TestLoadDLQEntries_FileNotFound(t *testing.T) {
	_, err := dlq.LoadDLQEntries("/nonexistent/file.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestInspect_EmptyEntries(t *testing.T) {
	result := dlq.Inspect(nil, dlq.DefaultConfig())
	if result.TotalEntries != 0 {
		t.Errorf("expected 0 total, got %d", result.TotalEntries)
	}
}

func TestInspect_GroupByReason(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_sample.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	cfg.GroupBy = dlq.GroupByReason
	result := dlq.Inspect(entries, cfg)

	if result.TotalEntries != 8 {
		t.Errorf("expected 8 total, got %d", result.TotalEntries)
	}
	if result.UniqueReasons < 3 {
		t.Errorf("expected at least 3 unique reasons, got %d", result.UniqueReasons)
	}
	if len(result.Patterns) == 0 {
		t.Fatal("expected at least one pattern")
	}
	// schema_validation_failed should be most common (5 entries)
	if result.Patterns[0].GroupKey != "schema_validation_failed" {
		t.Errorf("expected top pattern to be 'schema_validation_failed', got %q", result.Patterns[0].GroupKey)
	}
	if result.Patterns[0].Count != 5 {
		t.Errorf("expected count 5, got %d", result.Patterns[0].Count)
	}
}

func TestInspect_GroupByTopic(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_sample.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	cfg.GroupBy = dlq.GroupByTopic
	result := dlq.Inspect(entries, cfg)

	if len(result.Patterns) == 0 {
		t.Fatal("expected patterns")
	}
	// "orders" should be the top topic
	if result.Patterns[0].GroupKey != "orders" {
		t.Errorf("expected top topic 'orders', got %q", result.Patterns[0].GroupKey)
	}
}

func TestInspect_GroupByKey(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_sample.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	cfg.GroupBy = dlq.GroupByKey
	result := dlq.Inspect(entries, cfg)

	if len(result.Patterns) == 0 {
		t.Fatal("expected patterns")
	}
	// "order-101" appears 3 times, should be top
	if result.Patterns[0].GroupKey != "order-101" {
		t.Errorf("expected top key 'order-101', got %q", result.Patterns[0].GroupKey)
	}
}

func TestInspect_TopN(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_sample.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	cfg.TopN = 1
	result := dlq.Inspect(entries, cfg)

	if len(result.Patterns) != 1 {
		t.Errorf("expected 1 pattern with topN=1, got %d", len(result.Patterns))
	}
}

func TestInspect_ExamplesLimitedTo3(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_sample.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	result := dlq.Inspect(entries, cfg)

	for _, p := range result.Patterns {
		if len(p.Examples) > 3 {
			t.Errorf("pattern %q has %d examples, expected max 3", p.GroupKey, len(p.Examples))
		}
	}
}
