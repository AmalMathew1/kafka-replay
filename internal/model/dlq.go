package model

import "time"

// DLQEntry represents a dead letter queue event with failure metadata.
type DLQEntry struct {
	Event         Event             `json:"event"`
	FailureReason string            `json:"failure_reason"`
	OriginalTopic string            `json:"original_topic"`
	RetryCount    int               `json:"retry_count"`
	FirstFailedAt time.Time         `json:"first_failed_at"`
	LastFailedAt  time.Time         `json:"last_failed_at"`
	ErrorDetails  map[string]string `json:"error_details,omitempty"`
}

// FailurePattern describes a recurring failure pattern in DLQ data.
type FailurePattern struct {
	GroupKey   string
	Count     int
	Examples  []DLQEntry
	AvgRetry  float64
	MaxRetry  int
}

// InspectionResult holds the outcome of DLQ analysis.
type InspectionResult struct {
	TotalEntries    int
	UniqueReasons   int
	UniqueKeys      int
	Patterns        []FailurePattern
	PoisonPills     []PoisonPill
	RetryExhausted  []DLQEntry
	BurstFailures   []BurstInfo
}

// PoisonPill describes an event key that fails repeatedly.
type PoisonPill struct {
	Key       string
	Count     int
	Reasons   []string
}

// BurstInfo describes a cluster of failures within a short time window.
type BurstInfo struct {
	Start    time.Time
	End      time.Time
	Count    int
	Duration time.Duration
}
