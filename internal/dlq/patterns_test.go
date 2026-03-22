package dlq_test

import (
	"path/filepath"
	"testing"

	"github.com/AmalMathew1/kafka-replay/internal/dlq"
)

func TestPoisonPills_Detected(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_sample.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	cfg.PoisonPillThreshold = 3
	result := dlq.Inspect(entries, cfg)

	if len(result.PoisonPills) == 0 {
		t.Fatal("expected at least one poison pill")
	}
	// order-101 has 3 failures — should be detected
	found := false
	for _, pp := range result.PoisonPills {
		if pp.Key == "order-101" {
			found = true
			if pp.Count < 3 {
				t.Errorf("expected count >= 3 for order-101, got %d", pp.Count)
			}
			if len(pp.Reasons) < 2 {
				t.Errorf("expected multiple reasons for order-101, got %d", len(pp.Reasons))
			}
		}
	}
	if !found {
		t.Error("expected order-101 to be detected as poison pill")
	}
}

func TestPoisonPills_HighThreshold(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_sample.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	cfg.PoisonPillThreshold = 100 // very high
	result := dlq.Inspect(entries, cfg)

	if len(result.PoisonPills) != 0 {
		t.Errorf("expected 0 poison pills with high threshold, got %d", len(result.PoisonPills))
	}
}

func TestRetryExhausted_Detected(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_retries.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	cfg.RetryThreshold = 5
	result := dlq.Inspect(entries, cfg)

	if len(result.RetryExhausted) != 2 {
		t.Fatalf("expected 2 retry exhausted entries, got %d", len(result.RetryExhausted))
	}
	// Should be sorted by retry count descending
	if result.RetryExhausted[0].RetryCount < result.RetryExhausted[1].RetryCount {
		t.Error("expected retry exhausted sorted descending by retry count")
	}
}

func TestRetryExhausted_NoneAboveThreshold(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_retries.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	cfg.RetryThreshold = 100
	result := dlq.Inspect(entries, cfg)

	if len(result.RetryExhausted) != 0 {
		t.Errorf("expected 0 with high threshold, got %d", len(result.RetryExhausted))
	}
}

func TestBurstFailures_Detected(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_sample.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	cfg.BurstWindow = 60     // 60 seconds
	cfg.BurstMinCount = 3
	result := dlq.Inspect(entries, cfg)

	if len(result.BurstFailures) == 0 {
		t.Fatal("expected at least one burst (multiple failures within 60s window)")
	}
	for _, b := range result.BurstFailures {
		if b.Count < 3 {
			t.Errorf("burst count %d below minimum %d", b.Count, 3)
		}
	}
}

func TestBurstFailures_NoBurst(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_retries.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	cfg.BurstWindow = 10 // very small window
	cfg.BurstMinCount = 3
	result := dlq.Inspect(entries, cfg)

	if len(result.BurstFailures) != 0 {
		t.Errorf("expected no bursts with tight window, got %d", len(result.BurstFailures))
	}
}

func TestInspect_AvgAndMaxRetry(t *testing.T) {
	path := filepath.Join(testdataDir(), "dlq", "dlq_sample.json")
	entries, err := dlq.LoadDLQEntries(path)
	if err != nil {
		t.Fatalf("loading: %v", err)
	}

	cfg := dlq.DefaultConfig()
	result := dlq.Inspect(entries, cfg)

	for _, p := range result.Patterns {
		if p.AvgRetry < 0 {
			t.Errorf("pattern %q has negative avg retry", p.GroupKey)
		}
		if p.MaxRetry < 0 {
			t.Errorf("pattern %q has negative max retry", p.GroupKey)
		}
	}
}
