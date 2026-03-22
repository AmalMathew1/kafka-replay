package diff

import "github.com/AmalMathew1/kafka-replay/internal/model"

// ChangeType describes how an event differs between two streams.
type ChangeType string

const (
	Unchanged ChangeType = "unchanged"
	Modified  ChangeType = "modified"
	Added     ChangeType = "added"
	Removed   ChangeType = "removed"
)

// DiffEntry represents a single comparison result between two events.
type DiffEntry struct {
	Change ChangeType
	Left   *model.Event // nil for Added
	Right  *model.Event // nil for Removed
	Fields []FieldDiff  // populated for Modified
}

// FieldDiff describes a single field difference within a Modified event.
type FieldDiff struct {
	Path     string
	OldValue string
	NewValue string
}

// DiffResult holds the full comparison between two event streams.
type DiffResult struct {
	Entries       []DiffEntry
	AddedCount    int
	RemovedCount  int
	ModifiedCount int
	UnchangedCount int
}

// Summary returns a one-line summary string.
func (r DiffResult) Summary() string {
	return ""
}
