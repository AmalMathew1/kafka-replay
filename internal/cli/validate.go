package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/AmalMathew1/kafka-replay/internal/eventstore"
	"github.com/AmalMathew1/kafka-replay/internal/filter"
	"github.com/AmalMathew1/kafka-replay/internal/model"
	"github.com/AmalMathew1/kafka-replay/internal/validate"
)

func newValidateCmd() *cobra.Command {
	var (
		schemaPath string
		strict     bool
		eventType  string
	)

	cmd := &cobra.Command{
		Use:   "validate <file>",
		Short: "Validate events against a JSON Schema",
		Long:  `Check each event's payload against a JSON Schema and report validation errors.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runValidate(args[0], schemaPath, strict, eventType)
		},
	}

	cmd.Flags().StringVar(&schemaPath, "schema", "", "path to JSON Schema file (required)")
	cmd.Flags().BoolVar(&strict, "strict", false, "fail on first invalid event")
	cmd.Flags().StringVar(&eventType, "type", "", "only validate events of this type")
	_ = cmd.MarkFlagRequired("schema")

	return cmd
}

func runValidate(filePath, schemaPath string, strict bool, eventType string) error {
	store := eventstore.NewMemoryStore()
	if err := store.Load(filePath); err != nil {
		return fmt.Errorf("loading events: %w", err)
	}

	events := store.All()
	if eventType != "" {
		events = filter.Apply(events, model.FilterCriteria{
			EventTypes: []string{eventType},
		})
	}

	if len(events) == 0 {
		fmt.Fprintln(os.Stdout, "No events to validate.")
		return nil
	}

	result, err := validate.ValidateEvents(events, schemaPath, strict)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "Validated %d events: %d valid, %d invalid\n",
		result.TotalEvents, result.ValidCount, result.InvalidCount)

	for _, ve := range result.Errors {
		fmt.Fprintf(os.Stdout, "\n  Event #%d  key=%s  type=%s\n",
			ve.Index+1, ve.Event.Key, ve.Event.EventType)
		for _, e := range ve.Errors {
			fmt.Fprintf(os.Stdout, "    - %s\n", e)
		}
	}

	if result.InvalidCount > 0 {
		return fmt.Errorf("%d events failed validation", result.InvalidCount)
	}
	return nil
}
