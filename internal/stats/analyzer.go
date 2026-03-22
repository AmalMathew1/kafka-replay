package stats

import (
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// Analyze computes statistics for the given events.
// Events are assumed to be sorted by timestamp.
func Analyze(events []model.Event, windowSize time.Duration, gapThreshold time.Duration) model.StatsResult {
	if len(events) == 0 {
		return model.StatsResult{
			TypeCounts:     make(map[string]int),
			PartitionCount: make(map[int]int),
		}
	}

	result := model.StatsResult{
		TotalEvents:    len(events),
		TypeCounts:     countByType(events),
		PartitionCount: countByPartition(events),
		UniqueKeys:     countUniqueKeys(events),
		TimeRange:      computeTimeRange(events),
		Gaps:           detectGaps(events, gapThreshold),
		Duplicates:     detectDuplicates(events),
	}

	result.Throughput = computeThroughput(events, windowSize)

	return result
}

func countByType(events []model.Event) map[string]int {
	counts := make(map[string]int)
	for _, e := range events {
		counts[e.EventType]++
	}
	return counts
}

func countByPartition(events []model.Event) map[int]int {
	counts := make(map[int]int)
	for _, e := range events {
		counts[e.Partition]++
	}
	return counts
}

func countUniqueKeys(events []model.Event) int {
	seen := make(map[string]struct{})
	for _, e := range events {
		seen[e.Key] = struct{}{}
	}
	return len(seen)
}

func computeTimeRange(events []model.Event) model.TimeRange {
	first := events[0].Timestamp
	last := events[len(events)-1].Timestamp
	return model.TimeRange{
		From:     first,
		To:       last,
		Duration: last.Sub(first),
	}
}
