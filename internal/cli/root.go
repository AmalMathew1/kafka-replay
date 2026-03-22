package cli

import (
	"github.com/spf13/cobra"
)

// NewRootCmd creates the root CLI command.
func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kafka-replay",
		Short: "Kafka event replay and debug tool",
		Long: `A CLI tool for replaying, filtering, diffing, validating, and analyzing
Kafka event streams from exported files (JSON/Avro).

No live Kafka connection required — works entirely with exported event files.`,
	}

	cmd.AddCommand(newReplayCmd())

	return cmd
}
