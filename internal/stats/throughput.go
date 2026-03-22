package stats

import (
	"math"
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

func computeThroughput(events []model.Event, windowSize time.Duration) model.ThroughputStats {
	if len(events) == 0 || windowSize <= 0 {
		return model.ThroughputStats{WindowSize: windowSize}
	}

	first := events[0].Timestamp
	last := events[len(events)-1].Timestamp
	totalDuration := last.Sub(first)

	if totalDuration == 0 {
		return model.ThroughputStats{
			WindowSize: windowSize,
			Windows: []model.WindowCount{
				{Start: first, End: first.Add(windowSize), Count: len(events)},
			},
			Min:          len(events),
			Max:          len(events),
			Avg:          float64(len(events)),
			EventsPerSec: 0,
		}
	}

	windows := buildWindows(events, first, last, windowSize)

	minCount := math.MaxInt
	maxCount := 0
	totalCount := 0

	for _, w := range windows {
		if w.Count < minCount {
			minCount = w.Count
		}
		if w.Count > maxCount {
			maxCount = w.Count
		}
		totalCount += w.Count
	}

	avg := float64(totalCount) / float64(len(windows))
	eps := float64(len(events)) / totalDuration.Seconds()

	return model.ThroughputStats{
		WindowSize:   windowSize,
		Windows:      windows,
		Min:          minCount,
		Max:          maxCount,
		Avg:          avg,
		EventsPerSec: eps,
	}
}

func buildWindows(events []model.Event, start, end time.Time, windowSize time.Duration) []model.WindowCount {
	var windows []model.WindowCount
	windowStart := start
	eventIdx := 0

	for windowStart.Before(end) || windowStart.Equal(end) {
		windowEnd := windowStart.Add(windowSize)
		count := 0

		for eventIdx < len(events) && events[eventIdx].Timestamp.Before(windowEnd) {
			if !events[eventIdx].Timestamp.Before(windowStart) {
				count++
			}
			if events[eventIdx].Timestamp.Before(windowEnd) {
				eventIdx++
			} else {
				break
			}
		}

		windows = append(windows, model.WindowCount{
			Start: windowStart,
			End:   windowEnd,
			Count: count,
		})

		windowStart = windowEnd
		if len(windows) > 10000 {
			break
		}
	}

	return windows
}

func detectGaps(events []model.Event, threshold time.Duration) []model.GapInfo {
	var gaps []model.GapInfo
	for i := 1; i < len(events); i++ {
		gap := events[i].Timestamp.Sub(events[i-1].Timestamp)
		if gap >= threshold {
			gaps = append(gaps, model.GapInfo{
				After:        events[i-1].Timestamp,
				Before:       events[i].Timestamp,
				Duration:     gap,
				AfterOffset:  events[i-1].Offset,
				BeforeOffset: events[i].Offset,
			})
		}
	}
	return gaps
}

type duplicateKey struct {
	Key       string
	Partition int
	Offset    int64
}

func detectDuplicates(events []model.Event) []model.DuplicateInfo {
	counts := make(map[duplicateKey]int)
	for _, e := range events {
		k := duplicateKey{Key: e.Key, Partition: e.Partition, Offset: e.Offset}
		counts[k]++
	}

	var dups []model.DuplicateInfo
	for k, count := range counts {
		if count > 1 {
			dups = append(dups, model.DuplicateInfo{
				Key:       k.Key,
				Partition: k.Partition,
				Offset:    k.Offset,
				Count:     count,
			})
		}
	}
	return dups
}
