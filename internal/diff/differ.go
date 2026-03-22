package diff

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// Config holds diff configuration.
type Config struct {
	Strategy     AlignStrategy
	IgnoreFields []string
}

// Compare compares two event streams and returns a DiffResult.
func Compare(left, right []model.Event, cfg Config) (DiffResult, error) {
	pairs, err := Align(left, right, cfg.Strategy)
	if err != nil {
		return DiffResult{}, err
	}

	var result DiffResult
	for _, pair := range pairs {
		entry := classifyPair(pair, cfg.IgnoreFields)
		result.Entries = append(result.Entries, entry)

		switch entry.Change {
		case Added:
			result.AddedCount++
		case Removed:
			result.RemovedCount++
		case Modified:
			result.ModifiedCount++
		case Unchanged:
			result.UnchangedCount++
		}
	}

	return result, nil
}

func classifyPair(pair AlignedPair, ignoreFields []string) DiffEntry {
	if pair.Left == nil {
		return DiffEntry{Change: Added, Right: pair.Right}
	}
	if pair.Right == nil {
		return DiffEntry{Change: Removed, Left: pair.Left}
	}

	fields := compareValues(pair.Left.Value, pair.Right.Value, ignoreFields)
	if len(fields) == 0 {
		return DiffEntry{Change: Unchanged, Left: pair.Left, Right: pair.Right}
	}
	return DiffEntry{Change: Modified, Left: pair.Left, Right: pair.Right, Fields: fields}
}

func compareValues(leftRaw, rightRaw json.RawMessage, ignoreFields []string) []FieldDiff {
	var leftMap, rightMap map[string]any
	if err := json.Unmarshal(leftRaw, &leftMap); err != nil {
		return singleDiff("(root)", string(leftRaw), string(rightRaw))
	}
	if err := json.Unmarshal(rightRaw, &rightMap); err != nil {
		return singleDiff("(root)", string(leftRaw), string(rightRaw))
	}

	ignoreSet := make(map[string]bool, len(ignoreFields))
	for _, f := range ignoreFields {
		ignoreSet[f] = true
	}

	return diffMaps("", leftMap, rightMap, ignoreSet)
}

func diffMaps(prefix string, left, right map[string]any, ignore map[string]bool) []FieldDiff {
	var diffs []FieldDiff

	allKeys := make(map[string]bool)
	for k := range left {
		allKeys[k] = true
	}
	for k := range right {
		allKeys[k] = true
	}

	sortedKeys := make([]string, 0, len(allKeys))
	for k := range allKeys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, k := range sortedKeys {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}

		if ignore[path] || ignore[k] {
			continue
		}

		lv, lOk := left[k]
		rv, rOk := right[k]

		if !lOk {
			diffs = append(diffs, FieldDiff{Path: path, OldValue: "(absent)", NewValue: formatVal(rv)})
			continue
		}
		if !rOk {
			diffs = append(diffs, FieldDiff{Path: path, OldValue: formatVal(lv), NewValue: "(absent)"})
			continue
		}

		lMap, lIsMap := lv.(map[string]any)
		rMap, rIsMap := rv.(map[string]any)
		if lIsMap && rIsMap {
			diffs = append(diffs, diffMaps(path, lMap, rMap, ignore)...)
			continue
		}

		ls := formatVal(lv)
		rs := formatVal(rv)
		if ls != rs {
			diffs = append(diffs, FieldDiff{Path: path, OldValue: ls, NewValue: rs})
		}
	}

	return diffs
}

func formatVal(v any) string {
	if v == nil {
		return "null"
	}
	return fmt.Sprintf("%v", v)
}

func singleDiff(path, old, new_ string) []FieldDiff {
	if old == new_ {
		return nil
	}
	return []FieldDiff{{Path: path, OldValue: old, NewValue: new_}}
}
