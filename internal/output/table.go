package output

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// TableFormatter outputs data as aligned tab-separated columns.
type TableFormatter struct {
	tw *tabwriter.Writer
}

// NewTableFormatter creates a TableFormatter wrapping the given writer.
func NewTableFormatter(w io.Writer) *TableFormatter {
	return &TableFormatter{
		tw: tabwriter.NewWriter(w, 0, 0, 2, ' ', 0),
	}
}

func (f *TableFormatter) WriteHeader(w io.Writer, title string) error {
	_, err := fmt.Fprintf(f.tw, "=== %s ===\n", title)
	return err
}

func (f *TableFormatter) WriteKeyValue(w io.Writer, key, value string) error {
	_, err := fmt.Fprintf(f.tw, "%s:\t%s\n", key, value)
	return err
}

func (f *TableFormatter) WriteRow(w io.Writer, values ...string) error {
	_, err := fmt.Fprintln(f.tw, strings.Join(values, "\t"))
	return err
}

func (f *TableFormatter) WriteSeparator(w io.Writer) error {
	_, err := fmt.Fprintln(f.tw)
	return err
}

func (f *TableFormatter) Flush(w io.Writer) error {
	return f.tw.Flush()
}
