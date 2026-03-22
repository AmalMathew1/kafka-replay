package output

import (
	"encoding/json"
	"fmt"
	"io"
)

// WriteJSON marshals v as indented JSON and writes it to w.
func WriteJSON(w io.Writer, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling JSON output: %w", err)
	}
	_, err = fmt.Fprintln(w, string(data))
	return err
}
