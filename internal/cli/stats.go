package cli

import (
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/AmalMathew1/kafka-replay/internal/eventstore"
	"github.com/AmalMathew1/kafka-replay/internal/model"
	"github.com/AmalMathew1/kafka-replay/internal/output"
	"github.com/AmalMathew1/kafka-replay/internal/stats"
)

func newStatsCmd() *cobra.Command {
	var (
		window       time.Duration
		topN         int
		gapThreshold time.Duration
	)

	cmd := &cobra.Command{
		Use:   "stats <file>",
		Short: "Show event counts, throughput patterns, gaps, and duplicates",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStats(args[0], window, topN, gapThreshold)
		},
	}

	cmd.Flags().DurationVar(&window, "window", time.Minute, "throughput window size")
	cmd.Flags().IntVar(&topN, "top", 10, "top N event types to show")
	cmd.Flags().DurationVar(&gapThreshold, "detect-gaps", 5*time.Minute, "minimum gap to flag")

	return cmd
}

func runStats(filePath string, window time.Duration, topN int, gapThreshold time.Duration) error {
	store := eventstore.NewMemoryStore()
	if err := store.Load(filePath); err != nil {
		return fmt.Errorf("loading events: %w", err)
	}

	events := store.All()
	if len(events) == 0 {
		fmt.Fprintln(os.Stdout, "No events found.")
		return nil
	}

	result := stats.Analyze(events, window, gapThreshold)

	outFmt, err := getOutputFormat()
	if err != nil {
		return err
	}
	if outFmt == output.FormatJSON {
		return output.WriteJSON(os.Stdout, result)
	}

	printStats(result, topN)
	return nil
}

func printStats(r model.StatsResult, topN int) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Fprintf(w, "=== Summary ===\n")
	fmt.Fprintf(w, "Total Events:\t%d\n", r.TotalEvents)
	fmt.Fprintf(w, "Unique Keys:\t%d\n", r.UniqueKeys)
	fmt.Fprintf(w, "Time Range:\t%s → %s (%s)\n",
		r.TimeRange.From.Format(time.RFC3339),
		r.TimeRange.To.Format(time.RFC3339),
		r.TimeRange.Duration.Round(time.Second))
	fmt.Fprintf(w, "Events/sec:\t%.2f\n", r.Throughput.EventsPerSec)
	w.Flush()

	fmt.Fprintf(os.Stdout, "\n=== Event Types ===\n")
	printTypeCounts(r.TypeCounts, topN)

	fmt.Fprintf(os.Stdout, "\n=== Partitions ===\n")
	printPartitionCounts(r.PartitionCount)

	fmt.Fprintf(os.Stdout, "\n=== Throughput (window: %s) ===\n", r.Throughput.WindowSize)
	w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Min:\t%d events/window\n", r.Throughput.Min)
	fmt.Fprintf(w, "Max:\t%d events/window\n", r.Throughput.Max)
	fmt.Fprintf(w, "Avg:\t%.1f events/window\n", r.Throughput.Avg)
	w.Flush()

	if len(r.Gaps) > 0 {
		fmt.Fprintf(os.Stdout, "\n=== Gaps (>= threshold) ===\n")
		w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "After\tBefore\tDuration\n")
		fmt.Fprintf(w, "----\t------\t--------\n")
		for _, g := range r.Gaps {
			fmt.Fprintf(w, "%s\t%s\t%s\n",
				g.After.Format(time.RFC3339),
				g.Before.Format(time.RFC3339),
				g.Duration.Round(time.Second))
		}
		w.Flush()
	} else {
		fmt.Fprintf(os.Stdout, "\n=== Gaps ===\nNone detected.\n")
	}

	if len(r.Duplicates) > 0 {
		fmt.Fprintf(os.Stdout, "\n=== Duplicates ===\n")
		w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "Key\tPartition\tOffset\tCount\n")
		fmt.Fprintf(w, "---\t---------\t------\t-----\n")
		for _, d := range r.Duplicates {
			fmt.Fprintf(w, "%s\t%d\t%d\t%d\n", d.Key, d.Partition, d.Offset, d.Count)
		}
		w.Flush()
	} else {
		fmt.Fprintf(os.Stdout, "\n=== Duplicates ===\nNone detected.\n")
	}
}

type typeCount struct {
	Name  string
	Count int
}

func printTypeCounts(counts map[string]int, topN int) {
	sorted := make([]typeCount, 0, len(counts))
	for name, count := range counts {
		sorted = append(sorted, typeCount{Name: name, Count: count})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Count > sorted[j].Count
	})

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Type\tCount\n")
	fmt.Fprintf(w, "----\t-----\n")
	for i, tc := range sorted {
		if i >= topN {
			break
		}
		fmt.Fprintf(w, "%s\t%d\n", tc.Name, tc.Count)
	}
	w.Flush()
}

func printPartitionCounts(counts map[int]int) {
	keys := make([]int, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Partition\tCount\n")
	fmt.Fprintf(w, "---------\t-----\n")
	for _, k := range keys {
		fmt.Fprintf(w, "%d\t%d\n", k, counts[k])
	}
	w.Flush()
}
