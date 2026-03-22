package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/AmalMathew1/kafka-replay/internal/eventstore"
	"github.com/AmalMathew1/kafka-replay/internal/replay"
)

func newReplayCmd() *cobra.Command {
	var (
		speed  float64
		output string
	)

	cmd := &cobra.Command{
		Use:   "replay <file>",
		Short: "Replay events in timestamp order with timing simulation",
		Long: `Walk through events in order, simulating the original timing between events.
Use --speed to control playback speed (0 = no delay, 2 = twice as fast).`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReplay(args[0], speed, output)
		},
	}

	cmd.Flags().Float64Var(&speed, "speed", 1.0, "playback speed multiplier (0 = no delay)")
	cmd.Flags().StringVar(&output, "output", "full", "output detail: full, summary, key-only")

	return cmd
}

func runReplay(filePath string, speed float64, output string) error {
	outputMode, err := parseOutputMode(output)
	if err != nil {
		return err
	}

	store := eventstore.NewMemoryStore()
	if err := store.Load(filePath); err != nil {
		return fmt.Errorf("loading events: %w", err)
	}

	events := store.All()
	if len(events) == 0 {
		fmt.Fprintln(os.Stdout, "No events found.")
		return nil
	}

	fmt.Fprintf(os.Stdout, "Replaying %d events (speed: %.1fx)\n\n", len(events), speed)

	config := replay.DefaultConfig(os.Stdout)
	config.Speed = speed
	config.Output = outputMode

	replayer := replay.NewReplayer(config)
	return replayer.Replay(events)
}

func parseOutputMode(s string) (replay.OutputMode, error) {
	switch s {
	case "full":
		return replay.OutputFull, nil
	case "summary":
		return replay.OutputSummary, nil
	case "key-only":
		return replay.OutputKeyOnly, nil
	default:
		return "", fmt.Errorf("invalid output mode %q: must be full, summary, or key-only", s)
	}
}
