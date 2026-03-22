package cli

import (
	"github.com/spf13/cobra"

	"github.com/AmalMathew1/kafka-replay/internal/output"
)

var globalFormat string

// NewRootCmd creates the root CLI command.
func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kafka-replay",
		Short: "Kafka event replay and debug tool",
		Long: `A CLI tool for replaying, filtering, diffing, validating, and analyzing
Kafka event streams from exported files (JSON/Avro).

No live Kafka connection required — works entirely with exported event files.`,
	}

	cmd.PersistentFlags().StringVarP(&globalFormat, "format", "f", "table", "output format: table, json, plain")

	cmd.AddCommand(newReplayCmd())
	cmd.AddCommand(newFilterCmd())
	cmd.AddCommand(newStatsCmd())
	cmd.AddCommand(newDiffCmd())
	cmd.AddCommand(newValidateCmd())
	cmd.AddCommand(newDLQInspectCmd())

	return cmd
}

func getOutputFormat() (output.Format, error) {
	return output.ParseFormat(globalFormat)
}
