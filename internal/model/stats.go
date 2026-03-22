package model

import "time"

// StatsResult holds the computed statistics for an event stream.
type StatsResult struct {
	TotalEvents    int
	TypeCounts     map[string]int
	PartitionCount map[int]int
	UniqueKeys     int
	TimeRange      TimeRange
	Throughput     ThroughputStats
	Gaps           []GapInfo
	Duplicates     []DuplicateInfo
}

// TimeRange represents the start and end of an event stream.
type TimeRange struct {
	From     time.Time
	To       time.Time
	Duration time.Duration
}

// ThroughputStats holds windowed throughput metrics.
type ThroughputStats struct {
	WindowSize     time.Duration
	Windows        []WindowCount
	Min            int
	Max            int
	Avg            float64
	EventsPerSec   float64
}

// WindowCount holds event count for a single time window.
type WindowCount struct {
	Start time.Time
	End   time.Time
	Count int
}

// GapInfo describes a gap between consecutive events.
type GapInfo struct {
	After    time.Time
	Before   time.Time
	Duration time.Duration
	AfterOffset  int64
	BeforeOffset int64
}

// DuplicateInfo describes a duplicate event detected by key+partition+offset.
type DuplicateInfo struct {
	Key       string
	Partition int
	Offset    int64
	Count     int
}
