package output

import (
	"fmt"
	"io"
	"strings"
)

// PlainFormatter outputs data as simple key=value or space-separated text.
type PlainFormatter struct{}

func (f PlainFormatter) WriteHeader(w io.Writer, title string) error {
	_, err := fmt.Fprintf(w, "--- %s ---\n", title)
	return err
}

func (f PlainFormatter) WriteKeyValue(w io.Writer, key, value string) error {
	_, err := fmt.Fprintf(w, "%s: %s\n", key, value)
	return err
}

func (f PlainFormatter) WriteRow(w io.Writer, values ...string) error {
	_, err := fmt.Fprintln(w, strings.Join(values, " "))
	return err
}

func (f PlainFormatter) WriteSeparator(w io.Writer) error {
	_, err := fmt.Fprintln(w)
	return err
}

func (f PlainFormatter) Flush(w io.Writer) error {
	return nil
}
