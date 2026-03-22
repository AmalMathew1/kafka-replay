package diff_test

import (
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/AmalMathew1/kafka-replay/internal/diff"
	"github.com/AmalMathew1/kafka-replay/internal/model"
	"github.com/AmalMathew1/kafka-replay/internal/parser"
)

func testdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..", "testdata")
}

func TestCompare_IdenticalStreams(t *testing.T) {
	left := []model.Event{
		model.NewEvent("t", 0, 1, "k1", json.RawMessage(`{"a":1}`), nil, t0, "A"),
		model.NewEvent("t", 0, 2, "k2", json.RawMessage(`{"a":2}`), nil, t1, "A"),
	}

	result, err := diff.Compare(left, left, diff.Config{Strategy: diff.AlignByKey})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.UnchangedCount != 2 {
		t.Errorf("expected 2 unchanged, got %d", result.UnchangedCount)
	}
	if result.AddedCount != 0 || result.RemovedCount != 0 || result.ModifiedCount != 0 {
		t.Errorf("expected no changes, got added=%d removed=%d modified=%d",
			result.AddedCount, result.RemovedCount, result.ModifiedCount)
	}
}

func TestCompare_ModifiedEvent(t *testing.T) {
	left := []model.Event{
		model.NewEvent("t", 0, 1, "k1", json.RawMessage(`{"status":"created","amount":50}`), nil, t0, "A"),
	}
	right := []model.Event{
		model.NewEvent("t", 0, 1, "k1", json.RawMessage(`{"status":"confirmed","amount":50}`), nil, t0, "A"),
	}

	result, err := diff.Compare(left, right, diff.Config{Strategy: diff.AlignByKey})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.ModifiedCount != 1 {
		t.Fatalf("expected 1 modified, got %d", result.ModifiedCount)
	}
	if len(result.Entries[0].Fields) != 1 {
		t.Fatalf("expected 1 field diff, got %d", len(result.Entries[0].Fields))
	}
	if result.Entries[0].Fields[0].Path != "status" {
		t.Errorf("expected diff on 'status', got %q", result.Entries[0].Fields[0].Path)
	}
}

func TestCompare_AddedAndRemoved(t *testing.T) {
	left := []model.Event{
		model.NewEvent("t", 0, 1, "k1", json.RawMessage(`{"a":1}`), nil, t0, "A"),
		model.NewEvent("t", 0, 2, "k2", json.RawMessage(`{"a":2}`), nil, t1, "A"),
	}
	right := []model.Event{
		model.NewEvent("t", 0, 1, "k1", json.RawMessage(`{"a":1}`), nil, t0, "A"),
		model.NewEvent("t", 0, 3, "k3", json.RawMessage(`{"a":3}`), nil, t2, "A"),
	}

	result, err := diff.Compare(left, right, diff.Config{Strategy: diff.AlignByKey})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.UnchangedCount != 1 {
		t.Errorf("expected 1 unchanged, got %d", result.UnchangedCount)
	}
	if result.RemovedCount != 1 {
		t.Errorf("expected 1 removed, got %d", result.RemovedCount)
	}
	if result.AddedCount != 1 {
		t.Errorf("expected 1 added, got %d", result.AddedCount)
	}
}

func TestCompare_IgnoreFields(t *testing.T) {
	left := []model.Event{
		model.NewEvent("t", 0, 1, "k1", json.RawMessage(`{"status":"created","amount":50,"ts":"old"}`), nil, t0, "A"),
	}
	right := []model.Event{
		model.NewEvent("t", 0, 1, "k1", json.RawMessage(`{"status":"confirmed","amount":50,"ts":"new"}`), nil, t0, "A"),
	}

	result, err := diff.Compare(left, right, diff.Config{
		Strategy:     diff.AlignByKey,
		IgnoreFields: []string{"ts"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.ModifiedCount != 1 {
		t.Fatalf("expected 1 modified, got %d", result.ModifiedCount)
	}
	// Should only show status diff, not ts
	for _, fd := range result.Entries[0].Fields {
		if fd.Path == "ts" {
			t.Error("expected 'ts' to be ignored")
		}
	}
	if len(result.Entries[0].Fields) != 1 {
		t.Errorf("expected 1 field diff (status only), got %d", len(result.Entries[0].Fields))
	}
}

func TestCompare_NestedFieldDiff(t *testing.T) {
	left := []model.Event{
		model.NewEvent("t", 0, 1, "k1",
			json.RawMessage(`{"order":{"id":"1","status":"created"}}`), nil, t0, "A"),
	}
	right := []model.Event{
		model.NewEvent("t", 0, 1, "k1",
			json.RawMessage(`{"order":{"id":"1","status":"shipped"}}`), nil, t0, "A"),
	}

	result, err := diff.Compare(left, right, diff.Config{Strategy: diff.AlignByKey})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.ModifiedCount != 1 {
		t.Fatalf("expected 1 modified, got %d", result.ModifiedCount)
	}
	if result.Entries[0].Fields[0].Path != "order.status" {
		t.Errorf("expected nested path 'order.status', got %q", result.Entries[0].Fields[0].Path)
	}
}

func TestCompare_EmptyStreams(t *testing.T) {
	result, err := diff.Compare(nil, nil, diff.Config{Strategy: diff.AlignByKey})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(result.Entries))
	}
}

func TestCompare_FixtureFiles(t *testing.T) {
	pathA := filepath.Join(testdataDir(), "diff", "stream_a.json")
	pathB := filepath.Join(testdataDir(), "diff", "stream_b.json")

	left, err := parser.DetectAndParse(pathA)
	if err != nil {
		t.Fatalf("loading stream_a: %v", err)
	}
	right, err := parser.DetectAndParse(pathB)
	if err != nil {
		t.Fatalf("loading stream_b: %v", err)
	}

	result, err := diff.Compare(left, right, diff.Config{Strategy: diff.AlignByKey})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// stream_a: 101, 102, 103, 104
	// stream_b: 101 (same), 102 (modified), 105 (added)
	// So: 1 unchanged (101), 1 modified (102), 2 removed (103, 104), 1 added (105)
	if result.UnchangedCount != 1 {
		t.Errorf("expected 1 unchanged, got %d", result.UnchangedCount)
	}
	if result.ModifiedCount != 1 {
		t.Errorf("expected 1 modified, got %d", result.ModifiedCount)
	}
	if result.RemovedCount != 2 {
		t.Errorf("expected 2 removed, got %d", result.RemovedCount)
	}
	if result.AddedCount != 1 {
		t.Errorf("expected 1 added, got %d", result.AddedCount)
	}
}
