package cli

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/AmalMathew1/kafka-replay/internal/diff"
	"github.com/AmalMathew1/kafka-replay/internal/eventstore"
)

func newDiffCmd() *cobra.Command {
	var (
		alignBy string
		ignore  []string
		summary bool
	)

	cmd := &cobra.Command{
		Use:   "diff <file-a> <file-b>",
		Short: "Compare two event streams side by side",
		Long: `Align and compare two event streams, showing added, removed,
modified, and unchanged events with field-level detail.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDiff(args[0], args[1], alignBy, ignore, summary)
		},
	}

	cmd.Flags().StringVar(&alignBy, "align-by", "key", "alignment strategy: key, offset, timestamp")
	cmd.Flags().StringSliceVar(&ignore, "ignore", nil, "fields to ignore during comparison (repeatable)")
	cmd.Flags().BoolVar(&summary, "summary", false, "show only summary counts")

	return cmd
}

func runDiff(pathA, pathB, alignBy string, ignore []string, summaryOnly bool) error {
	strategy, err := parseAlignStrategy(alignBy)
	if err != nil {
		return err
	}

	storeA := eventstore.NewMemoryStore()
	if err := storeA.Load(pathA); err != nil {
		return fmt.Errorf("loading %s: %w", pathA, err)
	}
	storeB := eventstore.NewMemoryStore()
	if err := storeB.Load(pathB); err != nil {
		return fmt.Errorf("loading %s: %w", pathB, err)
	}

	result, err := diff.Compare(storeA.All(), storeB.All(), diff.Config{
		Strategy:     strategy,
		IgnoreFields: ignore,
	})
	if err != nil {
		return fmt.Errorf("comparing streams: %w", err)
	}

	printDiffSummary(result, pathA, pathB)

	if !summaryOnly {
		printDiffEntries(result)
	}

	return nil
}

func parseAlignStrategy(s string) (diff.AlignStrategy, error) {
	switch s {
	case "key":
		return diff.AlignByKey, nil
	case "offset":
		return diff.AlignByOffset, nil
	case "timestamp":
		return diff.AlignByTimestamp, nil
	default:
		return "", fmt.Errorf("invalid alignment strategy %q: must be key, offset, or timestamp", s)
	}
}

func printDiffSummary(r diff.DiffResult, pathA, pathB string) {
	total := r.AddedCount + r.RemovedCount + r.ModifiedCount + r.UnchangedCount
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "=== Diff Summary ===\n")
	fmt.Fprintf(w, "Left:\t%s\n", pathA)
	fmt.Fprintf(w, "Right:\t%s\n", pathB)
	fmt.Fprintf(w, "Total pairs:\t%d\n", total)
	fmt.Fprintf(w, "Unchanged:\t%d\n", r.UnchangedCount)
	fmt.Fprintf(w, "Modified:\t%d\n", r.ModifiedCount)
	fmt.Fprintf(w, "Added (right only):\t%d\n", r.AddedCount)
	fmt.Fprintf(w, "Removed (left only):\t%d\n", r.RemovedCount)
	w.Flush()
}

func printDiffEntries(r diff.DiffResult) {
	for _, entry := range r.Entries {
		switch entry.Change {
		case diff.Unchanged:
			continue
		case diff.Added:
			fmt.Fprintf(os.Stdout, "\n+ ADDED  key=%s  topic=%s  offset=%d\n",
				entry.Right.Key, entry.Right.Topic, entry.Right.Offset)
		case diff.Removed:
			fmt.Fprintf(os.Stdout, "\n- REMOVED  key=%s  topic=%s  offset=%d\n",
				entry.Left.Key, entry.Left.Topic, entry.Left.Offset)
		case diff.Modified:
			fmt.Fprintf(os.Stdout, "\n~ MODIFIED  key=%s\n", entry.Left.Key)
			for _, fd := range entry.Fields {
				fmt.Fprintf(os.Stdout, "    %s: %s → %s\n", fd.Path, fd.OldValue, fd.NewValue)
			}
		}
	}
}
