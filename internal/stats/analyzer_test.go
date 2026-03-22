package stats_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/model"
	"github.com/AmalMathew1/kafka-replay/internal/stats"
)

var (
	t0 = time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	t1 = time.Date(2024, 6, 15, 10, 0, 30, 0, time.UTC)
	t2 = time.Date(2024, 6, 15, 10, 1, 0, 0, time.UTC)
	t3 = time.Date(2024, 6, 15, 10, 1, 30, 0, time.UTC)
	t4 = time.Date(2024, 6, 15, 10, 10, 0, 0, time.UTC) // 8.5 min gap
)

func ev(key, eventType string, partition int, offset int64, ts time.Time) model.Event {
	return model.NewEvent("orders", partition, offset, key,
		json.RawMessage(`{"test":true}`), nil, ts, eventType)
}

func TestAnalyze_EmptyEvents(t *testing.T) {
	result := stats.Analyze(nil, time.Minute, 5*time.Minute)
	if result.TotalEvents != 0 {
		t.Errorf("expected 0 total, got %d", result.TotalEvents)
	}
	if result.TypeCounts == nil {
		t.Error("expected non-nil TypeCounts map")
	}
}

func TestAnalyze_TotalEvents(t *testing.T) {
	events := []model.Event{
		ev("k1", "OrderCreated", 0, 1, t0),
		ev("k2", "OrderCreated", 0, 2, t1),
		ev("k1", "OrderConfirmed", 1, 3, t2),
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)
	if result.TotalEvents != 3 {
		t.Errorf("expected 3 total, got %d", result.TotalEvents)
	}
}

func TestAnalyze_TypeCounts(t *testing.T) {
	events := []model.Event{
		ev("k1", "OrderCreated", 0, 1, t0),
		ev("k2", "OrderCreated", 0, 2, t1),
		ev("k1", "OrderConfirmed", 1, 3, t2),
		ev("k3", "PaymentProcessed", 0, 4, t3),
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if result.TypeCounts["OrderCreated"] != 2 {
		t.Errorf("expected 2 OrderCreated, got %d", result.TypeCounts["OrderCreated"])
	}
	if result.TypeCounts["OrderConfirmed"] != 1 {
		t.Errorf("expected 1 OrderConfirmed, got %d", result.TypeCounts["OrderConfirmed"])
	}
	if result.TypeCounts["PaymentProcessed"] != 1 {
		t.Errorf("expected 1 PaymentProcessed, got %d", result.TypeCounts["PaymentProcessed"])
	}
}

func TestAnalyze_PartitionCounts(t *testing.T) {
	events := []model.Event{
		ev("k1", "A", 0, 1, t0),
		ev("k2", "A", 0, 2, t1),
		ev("k3", "A", 1, 3, t2),
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if result.PartitionCount[0] != 2 {
		t.Errorf("expected 2 in partition 0, got %d", result.PartitionCount[0])
	}
	if result.PartitionCount[1] != 1 {
		t.Errorf("expected 1 in partition 1, got %d", result.PartitionCount[1])
	}
}

func TestAnalyze_UniqueKeys(t *testing.T) {
	events := []model.Event{
		ev("k1", "A", 0, 1, t0),
		ev("k2", "A", 0, 2, t1),
		ev("k1", "B", 0, 3, t2), // k1 again
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if result.UniqueKeys != 2 {
		t.Errorf("expected 2 unique keys, got %d", result.UniqueKeys)
	}
}

func TestAnalyze_TimeRange(t *testing.T) {
	events := []model.Event{
		ev("k1", "A", 0, 1, t0),
		ev("k2", "A", 0, 2, t3),
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if !result.TimeRange.From.Equal(t0) {
		t.Errorf("expected from %v, got %v", t0, result.TimeRange.From)
	}
	if !result.TimeRange.To.Equal(t3) {
		t.Errorf("expected to %v, got %v", t3, result.TimeRange.To)
	}
	expected := t3.Sub(t0)
	if result.TimeRange.Duration != expected {
		t.Errorf("expected duration %v, got %v", expected, result.TimeRange.Duration)
	}
}

func TestAnalyze_GapDetection(t *testing.T) {
	events := []model.Event{
		ev("k1", "A", 0, 1, t0),
		ev("k2", "A", 0, 2, t1),  // 30s gap — below threshold
		ev("k3", "A", 0, 3, t4),  // 9m30s gap — above threshold
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if len(result.Gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d", len(result.Gaps))
	}
	if result.Gaps[0].Duration != t4.Sub(t1) {
		t.Errorf("expected gap duration %v, got %v", t4.Sub(t1), result.Gaps[0].Duration)
	}
}

func TestAnalyze_NoGaps(t *testing.T) {
	events := []model.Event{
		ev("k1", "A", 0, 1, t0),
		ev("k2", "A", 0, 2, t1),
		ev("k3", "A", 0, 3, t2),
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if len(result.Gaps) != 0 {
		t.Errorf("expected 0 gaps, got %d", len(result.Gaps))
	}
}

func TestAnalyze_DuplicateDetection(t *testing.T) {
	events := []model.Event{
		ev("k1", "A", 0, 1, t0),
		ev("k1", "A", 0, 1, t1), // duplicate: same key+partition+offset
		ev("k2", "A", 0, 2, t2),
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if len(result.Duplicates) != 1 {
		t.Fatalf("expected 1 duplicate group, got %d", len(result.Duplicates))
	}
	if result.Duplicates[0].Count != 2 {
		t.Errorf("expected count 2, got %d", result.Duplicates[0].Count)
	}
}

func TestAnalyze_NoDuplicates(t *testing.T) {
	events := []model.Event{
		ev("k1", "A", 0, 1, t0),
		ev("k2", "A", 0, 2, t1),
	}
	result := stats.Analyze(events, time.Minute, 5*time.Minute)

	if len(result.Duplicates) != 0 {
		t.Errorf("expected 0 duplicates, got %d", len(result.Duplicates))
	}
}
