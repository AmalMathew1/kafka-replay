package stats_test

import (
	"encoding/json"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/model"
	"github.com/AmalMathew1/kafka-replay/internal/parser"
	"github.com/AmalMathew1/kafka-replay/internal/stats"
)

func testdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..", "testdata")
}

func TestThroughput_BasicWindows(t *testing.T) {
	events := []model.Event{
		model.NewEvent("t", 0, 1, "k1", json.RawMessage(`{}`), nil, t0, "A"),
		model.NewEvent("t", 0, 2, "k2", json.RawMessage(`{}`), nil, t0.Add(10*time.Second), "A"),
		model.NewEvent("t", 0, 3, "k3", json.RawMessage(`{}`), nil, t0.Add(20*time.Second), "A"),
		model.NewEvent("t", 0, 4, "k4", json.RawMessage(`{}`), nil, t0.Add(70*time.Second), "A"),
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if result.Throughput.WindowSize != time.Minute {
		t.Errorf("expected window size 1m, got %v", result.Throughput.WindowSize)
	}
	if len(result.Throughput.Windows) < 2 {
		t.Fatalf("expected at least 2 windows, got %d", len(result.Throughput.Windows))
	}
	if result.Throughput.Min < 1 {
		t.Errorf("expected min >= 1, got %d", result.Throughput.Min)
	}
	if result.Throughput.Max < 1 {
		t.Errorf("expected max >= 1, got %d", result.Throughput.Max)
	}
}

func TestThroughput_EventsPerSec(t *testing.T) {
	events := []model.Event{
		model.NewEvent("t", 0, 1, "k1", json.RawMessage(`{}`), nil, t0, "A"),
		model.NewEvent("t", 0, 2, "k2", json.RawMessage(`{}`), nil, t0.Add(time.Second), "A"),
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if result.Throughput.EventsPerSec != 2.0 {
		t.Errorf("expected 2.0 events/sec, got %.2f", result.Throughput.EventsPerSec)
	}
}

func TestThroughput_SingleEvent(t *testing.T) {
	events := []model.Event{
		model.NewEvent("t", 0, 1, "k1", json.RawMessage(`{}`), nil, t0, "A"),
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if result.Throughput.EventsPerSec != 0 {
		t.Errorf("expected 0 events/sec for single event, got %.2f", result.Throughput.EventsPerSec)
	}
}

func TestThroughput_LargeFixture(t *testing.T) {
	path := filepath.Join(testdataDir(), "events", "large_batch.jsonl")
	events, err := parser.DetectAndParse(path)
	if err != nil {
		t.Fatalf("failed to load fixture: %v", err)
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp.Before(events[j].Timestamp)
	})

	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if result.TotalEvents != 1000 {
		t.Errorf("expected 1000 events, got %d", result.TotalEvents)
	}
	if result.Throughput.EventsPerSec <= 0 {
		t.Error("expected positive events/sec")
	}
	if len(result.Throughput.Windows) == 0 {
		t.Error("expected at least one throughput window")
	}
	if len(result.Gaps) == 0 {
		t.Error("expected at least one gap in large fixture")
	}
	// Duplicates are detected by key+partition+offset; verify the check ran.
	t.Logf("Found %d duplicate groups", len(result.Duplicates))
}
