package filter

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ExtractField extracts a value from a JSON payload using dot-notation path.
// Returns the value and true if found, or nil and false if not found.
func ExtractField(raw json.RawMessage, path string) (any, bool) {
	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return nil, false
	}

	var current any
	if err := json.Unmarshal(raw, &current); err != nil {
		return nil, false
	}

	for _, part := range parts {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		val, exists := obj[part]
		if !exists {
			return nil, false
		}
		current = val
	}

	return current, true
}

// ExtractFieldString extracts a field and returns it as a string.
func ExtractFieldString(raw json.RawMessage, path string) (string, bool) {
	val, ok := ExtractField(raw, path)
	if !ok {
		return "", false
	}
	return fmt.Sprintf("%v", val), true
}
