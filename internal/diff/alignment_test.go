package diff_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/diff"
	"github.com/AmalMathew1/kafka-replay/internal/model"
)

var (
	t0 = time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	t1 = time.Date(2024, 6, 15, 10, 1, 0, 0, time.UTC)
	t2 = time.Date(2024, 6, 15, 10, 2, 0, 0, time.UTC)
	t3 = time.Date(2024, 6, 15, 10, 3, 0, 0, time.UTC)
)

func ev(key string, partition int, offset int64, ts time.Time) model.Event {
	return model.NewEvent("orders", partition, offset, key,
		json.RawMessage(`{"test":true}`), nil, ts, "TestEvent")
}

func TestAlignByKey_MatchesSameKey(t *testing.T) {
	left := []model.Event{ev("k1", 0, 1, t0), ev("k2", 0, 2, t1)}
	right := []model.Event{ev("k1", 0, 1, t0), ev("k2", 0, 2, t1)}

	pairs, err := diff.Align(left, right, diff.AlignByKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pairs) != 2 {
		t.Fatalf("expected 2 pairs, got %d", len(pairs))
	}
	for _, p := range pairs {
		if p.Left == nil || p.Right == nil {
			t.Error("expected both sides to be matched")
		}
	}
}

func TestAlignByKey_AddedAndRemoved(t *testing.T) {
	left := []model.Event{ev("k1", 0, 1, t0), ev("k3", 0, 3, t2)}
	right := []model.Event{ev("k1", 0, 1, t0), ev("k4", 0, 4, t3)}

	pairs, err := diff.Align(left, right, diff.AlignByKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// k1 matched, k3 removed (left only), k4 added (right only)
	if len(pairs) != 3 {
		t.Fatalf("expected 3 pairs, got %d", len(pairs))
	}

	var matched, removed, added int
	for _, p := range pairs {
		if p.Left != nil && p.Right != nil {
			matched++
		} else if p.Left != nil {
			removed++
		} else {
			added++
		}
	}
	if matched != 1 || removed != 1 || added != 1 {
		t.Errorf("expected 1 matched, 1 removed, 1 added; got %d, %d, %d", matched, removed, added)
	}
}

func TestAlignByKey_DuplicateKeys(t *testing.T) {
	left := []model.Event{ev("k1", 0, 1, t0), ev("k1", 0, 2, t1)}
	right := []model.Event{ev("k1", 0, 1, t0), ev("k1", 0, 2, t1)}

	pairs, err := diff.Align(left, right, diff.AlignByKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pairs) != 2 {
		t.Fatalf("expected 2 pairs, got %d", len(pairs))
	}
	// First-match ordering: left[0]->right[0], left[1]->right[1]
	for _, p := range pairs {
		if p.Left == nil || p.Right == nil {
			t.Error("expected both sides matched for duplicate keys")
		}
	}
}

func TestAlignByOffset_MatchesSamePartitionOffset(t *testing.T) {
	left := []model.Event{ev("k1", 0, 1, t0), ev("k2", 0, 2, t1)}
	right := []model.Event{ev("kx", 0, 1, t0), ev("ky", 0, 2, t1)}

	pairs, err := diff.Align(left, right, diff.AlignByOffset)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pairs) != 2 {
		t.Fatalf("expected 2 pairs, got %d", len(pairs))
	}
	// Matched by partition+offset, not key
	if pairs[0].Left.Key != "k1" || pairs[0].Right.Key != "kx" {
		t.Errorf("unexpected first pair: %s <-> %s", pairs[0].Left.Key, pairs[0].Right.Key)
	}
}

func TestAlignByTimestamp_MatchesClosest(t *testing.T) {
	left := []model.Event{ev("k1", 0, 1, t0)}
	right := []model.Event{ev("k2", 0, 2, t0.Add(500 * time.Millisecond))} // within 1s

	pairs, err := diff.Align(left, right, diff.AlignByTimestamp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(pairs))
	}
	if pairs[0].Left == nil || pairs[0].Right == nil {
		t.Error("expected match within 1s tolerance")
	}
}

func TestAlignByTimestamp_TooFarApart(t *testing.T) {
	left := []model.Event{ev("k1", 0, 1, t0)}
	right := []model.Event{ev("k2", 0, 2, t0.Add(2 * time.Second))} // beyond 1s

	pairs, err := diff.Align(left, right, diff.AlignByTimestamp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not match; both become unmatched
	if len(pairs) != 2 {
		t.Fatalf("expected 2 pairs (unmatched), got %d", len(pairs))
	}
}

func TestAlign_InvalidStrategy(t *testing.T) {
	_, err := diff.Align(nil, nil, "invalid")
	if err == nil {
		t.Fatal("expected error for invalid strategy")
	}
}

func TestAlignByKey_EmptyStreams(t *testing.T) {
	pairs, err := diff.Align(nil, nil, diff.AlignByKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pairs) != 0 {
		t.Errorf("expected 0 pairs, got %d", len(pairs))
	}
}

func TestAlignByKey_OneEmpty(t *testing.T) {
	left := []model.Event{ev("k1", 0, 1, t0)}

	pairs, err := diff.Align(left, nil, diff.AlignByKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(pairs))
	}
	if pairs[0].Left == nil || pairs[0].Right != nil {
		t.Error("expected left-only pair")
	}
}
