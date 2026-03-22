package output

import (
	"fmt"
	"io"
)

// Format represents an output format.
type Format string

const (
	FormatTable Format = "table"
	FormatJSON  Format = "json"
	FormatPlain Format = "plain"
)

// ParseFormat parses a format string, returning an error for unknown formats.
func ParseFormat(s string) (Format, error) {
	switch s {
	case "table", "":
		return FormatTable, nil
	case "json":
		return FormatJSON, nil
	case "plain":
		return FormatPlain, nil
	default:
		return "", fmt.Errorf("unknown format %q: must be table, json, or plain", s)
	}
}

// Formatter writes structured output in a specific format.
type Formatter interface {
	WriteHeader(w io.Writer, title string) error
	WriteKeyValue(w io.Writer, key, value string) error
	WriteRow(w io.Writer, values ...string) error
	WriteSeparator(w io.Writer) error
	Flush(w io.Writer) error
}
