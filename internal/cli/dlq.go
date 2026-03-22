package cli

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/AmalMathew1/kafka-replay/internal/dlq"
	"github.com/AmalMathew1/kafka-replay/internal/model"
	"github.com/AmalMathew1/kafka-replay/internal/output"
)

func newDLQInspectCmd() *cobra.Command {
	var (
		groupBy    string
		topN       int
		showEvents bool
	)

	cmd := &cobra.Command{
		Use:   "dlq-inspect <file>",
		Short: "Analyze dead letter queue dumps for failure patterns",
		Long: `Parse a DLQ dump file and detect failure patterns including
poison pills, retry exhaustion, and burst failures.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDLQInspect(args[0], groupBy, topN, showEvents)
		},
	}

	cmd.Flags().StringVar(&groupBy, "group-by", "reason", "group failures by: reason, topic, key")
	cmd.Flags().IntVar(&topN, "top", 10, "top N patterns to show")
	cmd.Flags().BoolVar(&showEvents, "show-events", false, "show individual events per pattern")

	return cmd
}

func runDLQInspect(filePath, groupBy string, topN int, showEvents bool) error {
	gb, err := parseDLQGroupBy(groupBy)
	if err != nil {
		return err
	}

	entries, err := dlq.LoadDLQEntries(filePath)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		fmt.Fprintln(os.Stdout, "No DLQ entries found.")
		return nil
	}

	cfg := dlq.DefaultConfig()
	cfg.GroupBy = gb
	cfg.TopN = topN

	result := dlq.Inspect(entries, cfg)

	outFmt, err := getOutputFormat()
	if err != nil {
		return err
	}
	if outFmt == output.FormatJSON {
		return output.WriteJSON(os.Stdout, result)
	}

	printDLQResult(result, showEvents)
	return nil
}

func parseDLQGroupBy(s string) (dlq.GroupBy, error) {
	switch s {
	case "reason":
		return dlq.GroupByReason, nil
	case "topic":
		return dlq.GroupByTopic, nil
	case "key":
		return dlq.GroupByKey, nil
	default:
		return "", fmt.Errorf("invalid --group-by %q: must be reason, topic, or key", s)
	}
}

func printDLQResult(r model.InspectionResult, showEvents bool) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "=== DLQ Summary ===\n")
	fmt.Fprintf(w, "Total Entries:\t%d\n", r.TotalEntries)
	fmt.Fprintf(w, "Unique Reasons:\t%d\n", r.UniqueReasons)
	fmt.Fprintf(w, "Unique Keys:\t%d\n", r.UniqueKeys)
	w.Flush()

	if len(r.Patterns) > 0 {
		fmt.Fprintf(os.Stdout, "\n=== Failure Patterns ===\n")
		w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "Group\tCount\tAvg Retries\tMax Retries\n")
		fmt.Fprintf(w, "-----\t-----\t-----------\t-----------\n")
		for _, p := range r.Patterns {
			fmt.Fprintf(w, "%s\t%d\t%.1f\t%d\n", p.GroupKey, p.Count, p.AvgRetry, p.MaxRetry)
		}
		w.Flush()

		if showEvents {
			for _, p := range r.Patterns {
				fmt.Fprintf(os.Stdout, "\n  [%s] examples:\n", p.GroupKey)
				for _, ex := range p.Examples {
					fmt.Fprintf(os.Stdout, "    key=%s topic=%s retries=%d reason=%s\n",
						ex.Event.Key, ex.OriginalTopic, ex.RetryCount, ex.FailureReason)
				}
			}
		}
	}

	if len(r.PoisonPills) > 0 {
		fmt.Fprintf(os.Stdout, "\n=== Poison Pills ===\n")
		w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "Key\tFailures\tReasons\n")
		fmt.Fprintf(w, "---\t--------\t-------\n")
		for _, pp := range r.PoisonPills {
			fmt.Fprintf(w, "%s\t%d\t%v\n", pp.Key, pp.Count, pp.Reasons)
		}
		w.Flush()
	}

	if len(r.RetryExhausted) > 0 {
		fmt.Fprintf(os.Stdout, "\n=== Retry Exhausted ===\n")
		w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "Key\tRetries\tReason\tLast Failed\n")
		fmt.Fprintf(w, "---\t-------\t------\t-----------\n")
		for _, e := range r.RetryExhausted {
			fmt.Fprintf(w, "%s\t%d\t%s\t%s\n",
				e.Event.Key, e.RetryCount, e.FailureReason, e.LastFailedAt.Format(time.RFC3339))
		}
		w.Flush()
	}

	if len(r.BurstFailures) > 0 {
		fmt.Fprintf(os.Stdout, "\n=== Burst Failures ===\n")
		w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "Start\tEnd\tCount\tDuration\n")
		fmt.Fprintf(w, "-----\t---\t-----\t--------\n")
		for _, b := range r.BurstFailures {
			fmt.Fprintf(w, "%s\t%s\t%d\t%s\n",
				b.Start.Format(time.RFC3339), b.End.Format(time.RFC3339),
				b.Count, b.Duration.Round(time.Second))
		}
		w.Flush()
	}
}
