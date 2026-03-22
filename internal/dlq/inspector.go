package dlq

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// GroupBy defines how to group DLQ failures.
type GroupBy string

const (
	GroupByReason GroupBy = "reason"
	GroupByTopic  GroupBy = "topic"
	GroupByKey    GroupBy = "key"
)

// Config holds DLQ inspection configuration.
type Config struct {
	GroupBy            GroupBy
	TopN               int
	PoisonPillThreshold int // minimum failures for a key to be a poison pill
	RetryThreshold      int // minimum retry count for "retry exhausted"
	BurstWindow         int // seconds; failures within this window = burst
	BurstMinCount       int // minimum failures in window to flag
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		GroupBy:            GroupByReason,
		TopN:               10,
		PoisonPillThreshold: 3,
		RetryThreshold:      5,
		BurstWindow:         60,
		BurstMinCount:       3,
	}
}

// LoadDLQEntries loads DLQ entries from a JSON file.
func LoadDLQEntries(filePath string) ([]model.DLQEntry, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("reading DLQ file %s: %w", filePath, err)
	}

	var entries []model.DLQEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parsing DLQ entries: %w", err)
	}

	return entries, nil
}

// Inspect analyzes DLQ entries and returns an InspectionResult.
func Inspect(entries []model.DLQEntry, cfg Config) model.InspectionResult {
	if len(entries) == 0 {
		return model.InspectionResult{}
	}

	result := model.InspectionResult{
		TotalEntries: len(entries),
		UniqueReasons: countUnique(entries, func(e model.DLQEntry) string { return e.FailureReason }),
		UniqueKeys:    countUnique(entries, func(e model.DLQEntry) string { return e.Event.Key }),
		Patterns:      groupEntries(entries, cfg),
		PoisonPills:   detectPoisonPills(entries, cfg.PoisonPillThreshold),
		RetryExhausted: detectRetryExhausted(entries, cfg.RetryThreshold),
		BurstFailures:  detectBursts(entries, cfg.BurstWindow, cfg.BurstMinCount),
	}

	return result
}

func countUnique(entries []model.DLQEntry, keyFn func(model.DLQEntry) string) int {
	seen := make(map[string]struct{})
	for _, e := range entries {
		seen[keyFn(e)] = struct{}{}
	}
	return len(seen)
}

func groupEntries(entries []model.DLQEntry, cfg Config) []model.FailurePattern {
	groups := make(map[string][]model.DLQEntry)
	for _, e := range entries {
		var key string
		switch cfg.GroupBy {
		case GroupByTopic:
			key = e.OriginalTopic
		case GroupByKey:
			key = e.Event.Key
		default:
			key = e.FailureReason
		}
		groups[key] = append(groups[key], e)
	}

	patterns := make([]model.FailurePattern, 0, len(groups))
	for key, group := range groups {
		totalRetry := 0
		maxRetry := 0
		for _, e := range group {
			totalRetry += e.RetryCount
			if e.RetryCount > maxRetry {
				maxRetry = e.RetryCount
			}
		}

		examples := group
		if len(examples) > 3 {
			examples = examples[:3]
		}

		patterns = append(patterns, model.FailurePattern{
			GroupKey: key,
			Count:   len(group),
			Examples: examples,
			AvgRetry: float64(totalRetry) / float64(len(group)),
			MaxRetry: maxRetry,
		})
	}

	sort.Slice(patterns, func(i, j int) bool {
		return patterns[i].Count > patterns[j].Count
	})

	if cfg.TopN > 0 && len(patterns) > cfg.TopN {
		patterns = patterns[:cfg.TopN]
	}

	return patterns
}
