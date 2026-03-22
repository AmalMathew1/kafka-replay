package dlq

import (
	"sort"
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// detectPoisonPills finds keys that fail repeatedly (>= threshold).
func detectPoisonPills(entries []model.DLQEntry, threshold int) []model.PoisonPill {
	keyFailures := make(map[string][]model.DLQEntry)
	for _, e := range entries {
		keyFailures[e.Event.Key] = append(keyFailures[e.Event.Key], e)
	}

	var pills []model.PoisonPill
	for key, group := range keyFailures {
		if len(group) < threshold {
			continue
		}

		reasonSet := make(map[string]struct{})
		for _, e := range group {
			reasonSet[e.FailureReason] = struct{}{}
		}
		reasons := make([]string, 0, len(reasonSet))
		for r := range reasonSet {
			reasons = append(reasons, r)
		}
		sort.Strings(reasons)

		pills = append(pills, model.PoisonPill{
			Key:     key,
			Count:   len(group),
			Reasons: reasons,
		})
	}

	sort.Slice(pills, func(i, j int) bool {
		return pills[i].Count > pills[j].Count
	})

	return pills
}

// detectRetryExhausted finds entries with retry count >= threshold.
func detectRetryExhausted(entries []model.DLQEntry, threshold int) []model.DLQEntry {
	var exhausted []model.DLQEntry
	for _, e := range entries {
		if e.RetryCount >= threshold {
			exhausted = append(exhausted, e)
		}
	}

	sort.Slice(exhausted, func(i, j int) bool {
		return exhausted[i].RetryCount > exhausted[j].RetryCount
	})

	return exhausted
}

// detectBursts finds clusters of failures within a time window.
func detectBursts(entries []model.DLQEntry, windowSeconds, minCount int) []model.BurstInfo {
	if len(entries) < minCount {
		return nil
	}

	sorted := make([]model.DLQEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LastFailedAt.Before(sorted[j].LastFailedAt)
	})

	window := time.Duration(windowSeconds) * time.Second
	var bursts []model.BurstInfo

	i := 0
	for i < len(sorted) {
		windowStart := sorted[i].LastFailedAt
		windowEnd := windowStart.Add(window)

		j := i
		for j < len(sorted) && sorted[j].LastFailedAt.Before(windowEnd) {
			j++
		}

		count := j - i
		if count >= minCount {
			bursts = append(bursts, model.BurstInfo{
				Start:    sorted[i].LastFailedAt,
				End:      sorted[j-1].LastFailedAt,
				Count:    count,
				Duration: sorted[j-1].LastFailedAt.Sub(sorted[i].LastFailedAt),
			})
			i = j // skip past this burst
		} else {
			i++
		}
	}

	return bursts
}
