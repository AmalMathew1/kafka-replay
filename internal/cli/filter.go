package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/AmalMathew1/kafka-replay/internal/eventstore"
	"github.com/AmalMathew1/kafka-replay/internal/filter"
	"github.com/AmalMathew1/kafka-replay/internal/model"
)

func newFilterCmd() *cobra.Command {
	var (
		eventTypes []string
		from       string
		to         string
		keys       []string
		fields     []string
		operator   string
		countOnly  bool
		outputFile string
	)

	cmd := &cobra.Command{
		Use:   "filter <file>",
		Short: "Filter events by type, timestamp, key, or custom field",
		Long: `Filter events from a file by one or more criteria.
Multiple values within a flag use OR; different flags use AND.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			criteria, err := buildCriteria(eventTypes, from, to, keys, fields, operator)
			if err != nil {
				return err
			}
			return runFilter(args[0], criteria, countOnly, outputFile)
		},
	}

	cmd.Flags().StringSliceVar(&eventTypes, "type", nil, "event type (repeatable)")
	cmd.Flags().StringVar(&from, "from", "", "start timestamp (RFC3339)")
	cmd.Flags().StringVar(&to, "to", "", "end timestamp (RFC3339)")
	cmd.Flags().StringSliceVar(&keys, "key", nil, "event key (repeatable)")
	cmd.Flags().StringSliceVar(&fields, "field", nil, "field filter as path=value (repeatable)")
	cmd.Flags().StringVar(&operator, "operator", "eq", "operator for --field: eq|contains|gt|lt|exists")
	cmd.Flags().BoolVar(&countOnly, "count", false, "only print count, not events")
	cmd.Flags().StringVar(&outputFile, "output-file", "", "write results to file")

	return cmd
}

func buildCriteria(eventTypes []string, from, to string, keys, fields []string, operator string) (model.FilterCriteria, error) {
	criteria := model.FilterCriteria{
		EventTypes: eventTypes,
		Keys:       keys,
	}

	if from != "" {
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			return criteria, fmt.Errorf("invalid --from timestamp: %w", err)
		}
		criteria.TimeFrom = &t
	}

	if to != "" {
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			return criteria, fmt.Errorf("invalid --to timestamp: %w", err)
		}
		criteria.TimeTo = &t
	}

	for _, f := range fields {
		ff, err := filter.FormatFieldFilter(f, operator)
		if err != nil {
			return criteria, err
		}
		criteria.FieldFilters = append(criteria.FieldFilters, ff)
	}

	return criteria, nil
}

func runFilter(filePath string, criteria model.FilterCriteria, countOnly bool, outputFile string) error {
	store := eventstore.NewMemoryStore()
	if err := store.Load(filePath); err != nil {
		return fmt.Errorf("loading events: %w", err)
	}

	events := store.All()
	filtered := filter.Apply(events, criteria)

	if countOnly {
		fmt.Fprintf(os.Stdout, "%d\n", len(filtered))
		return nil
	}

	output, err := json.MarshalIndent(filtered, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling results: %w", err)
	}

	if outputFile != "" {
		if err := os.WriteFile(outputFile, output, 0644); err != nil {
			return fmt.Errorf("writing output file: %w", err)
		}
		fmt.Fprintf(os.Stdout, "Wrote %d events to %s\n", len(filtered), outputFile)
		return nil
	}

	fmt.Fprintln(os.Stdout, string(output))
	return nil
}
