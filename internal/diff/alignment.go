package diff

import (
	"fmt"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// AlignStrategy defines how to match events between two streams.
type AlignStrategy string

const (
	AlignByKey       AlignStrategy = "key"
	AlignByOffset    AlignStrategy = "offset"
	AlignByTimestamp AlignStrategy = "timestamp"
)

// AlignedPair represents a matched pair from two streams.
type AlignedPair struct {
	Left  *model.Event
	Right *model.Event
}

// Align matches events from two streams using the given strategy.
// Returns aligned pairs plus unmatched events from each side.
func Align(left, right []model.Event, strategy AlignStrategy) ([]AlignedPair, error) {
	switch strategy {
	case AlignByKey:
		return alignByKey(left, right), nil
	case AlignByOffset:
		return alignByOffset(left, right), nil
	case AlignByTimestamp:
		return alignByTimestamp(left, right), nil
	default:
		return nil, fmt.Errorf("unknown alignment strategy: %q", strategy)
	}
}

func alignByKey(left, right []model.Event) []AlignedPair {
	rightByKey := make(map[string][]int)
	for i := range right {
		rightByKey[right[i].Key] = append(rightByKey[right[i].Key], i)
	}

	matched := make(map[int]bool)
	var pairs []AlignedPair

	for i := range left {
		indices, ok := rightByKey[left[i].Key]
		if ok && len(indices) > 0 {
			ri := indices[0]
			rightByKey[left[i].Key] = indices[1:]
			matched[ri] = true
			l, r := left[i], right[ri]
			pairs = append(pairs, AlignedPair{Left: &l, Right: &r})
		} else {
			l := left[i]
			pairs = append(pairs, AlignedPair{Left: &l, Right: nil})
		}
	}

	for i := range right {
		if !matched[i] {
			r := right[i]
			pairs = append(pairs, AlignedPair{Left: nil, Right: &r})
		}
	}

	return pairs
}

func alignByOffset(left, right []model.Event) []AlignedPair {
	type offsetKey struct {
		Partition int
		Offset    int64
	}

	rightByOffset := make(map[offsetKey]int)
	for i := range right {
		k := offsetKey{right[i].Partition, right[i].Offset}
		rightByOffset[k] = i
	}

	matched := make(map[int]bool)
	var pairs []AlignedPair

	for i := range left {
		k := offsetKey{left[i].Partition, left[i].Offset}
		if ri, ok := rightByOffset[k]; ok {
			matched[ri] = true
			l, r := left[i], right[ri]
			pairs = append(pairs, AlignedPair{Left: &l, Right: &r})
		} else {
			l := left[i]
			pairs = append(pairs, AlignedPair{Left: &l, Right: nil})
		}
	}

	for i := range right {
		if !matched[i] {
			r := right[i]
			pairs = append(pairs, AlignedPair{Left: nil, Right: &r})
		}
	}

	return pairs
}

func alignByTimestamp(left, right []model.Event) []AlignedPair {
	matched := make(map[int]bool)
	var pairs []AlignedPair

	for i := range left {
		bestIdx := -1
		var bestDelta int64

		for j := range right {
			if matched[j] {
				continue
			}
			delta := absDurationNs(int64(left[i].Timestamp.Sub(right[j].Timestamp)))
			if bestIdx == -1 || delta < bestDelta {
				bestIdx = j
				bestDelta = delta
			}
		}

		if bestIdx >= 0 && bestDelta <= 1_000_000_000 { // within 1 second
			matched[bestIdx] = true
			l, r := left[i], right[bestIdx]
			pairs = append(pairs, AlignedPair{Left: &l, Right: &r})
		} else {
			l := left[i]
			pairs = append(pairs, AlignedPair{Left: &l, Right: nil})
		}
	}

	for i := range right {
		if !matched[i] {
			r := right[i]
			pairs = append(pairs, AlignedPair{Left: nil, Right: &r})
		}
	}

	return pairs
}

func absDurationNs(d int64) int64 {
	if d < 0 {
		return -d
	}
	return d
}
