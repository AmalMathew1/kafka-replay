package replay_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/AmalMathew1/kafka-replay/internal/model"
	"github.com/AmalMathew1/kafka-replay/internal/replay"
)

func makeEvent(key string, eventType string, ts time.Time) model.Event {
	return model.NewEvent("orders", 0, 1, key,
		json.RawMessage(`{"test": true}`),
		map[string]string{"source": "test"},
		ts, eventType)
}

func TestReplayer_DelayBetweenEvents(t *testing.T) {
	t0 := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	events := []model.Event{
		makeEvent("k1", "A", t0),
		makeEvent("k2", "B", t0.Add(2*time.Second)),
		makeEvent("k3", "C", t0.Add(5*time.Second)),
	}

	clock := &replay.FakeClock{}
	var buf bytes.Buffer
	config := replay.Config{
		Speed:  1.0,
		Output: replay.OutputKeyOnly,
		Writer: &buf,
		Clock:  clock,
	}

	r := replay.NewReplayer(config)
	if err := r.Replay(events); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(clock.Sleeps) != 2 {
		t.Fatalf("expected 2 sleeps, got %d", len(clock.Sleeps))
	}
	if clock.Sleeps[0] != 2*time.Second {
		t.Errorf("expected first sleep 2s, got %v", clock.Sleeps[0])
	}
	if clock.Sleeps[1] != 3*time.Second {
		t.Errorf("expected second sleep 3s, got %v", clock.Sleeps[1])
	}
}

func TestReplayer_SpeedMultiplier(t *testing.T) {
	t0 := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	events := []model.Event{
		makeEvent("k1", "A", t0),
		makeEvent("k2", "B", t0.Add(10*time.Second)),
	}

	clock := &replay.FakeClock{}
	var buf bytes.Buffer
	config := replay.Config{
		Speed:  2.0,
		Output: replay.OutputKeyOnly,
		Writer: &buf,
		Clock:  clock,
	}

	r := replay.NewReplayer(config)
	if err := r.Replay(events); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(clock.Sleeps) != 1 {
		t.Fatalf("expected 1 sleep, got %d", len(clock.Sleeps))
	}
	if clock.Sleeps[0] != 5*time.Second {
		t.Errorf("expected sleep 5s (10s/2x), got %v", clock.Sleeps[0])
	}
}

func TestReplayer_ZeroSpeed_NoDelay(t *testing.T) {
	t0 := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	events := []model.Event{
		makeEvent("k1", "A", t0),
		makeEvent("k2", "B", t0.Add(10*time.Second)),
	}

	clock := &replay.FakeClock{}
	var buf bytes.Buffer
	config := replay.Config{
		Speed:  0,
		Output: replay.OutputKeyOnly,
		Writer: &buf,
		Clock:  clock,
	}

	r := replay.NewReplayer(config)
	if err := r.Replay(events); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(clock.Sleeps) != 0 {
		t.Errorf("expected no sleeps at speed=0, got %d", len(clock.Sleeps))
	}
}

func TestReplayer_SingleEvent_NoSleep(t *testing.T) {
	t0 := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	events := []model.Event{
		makeEvent("k1", "A", t0),
	}

	clock := &replay.FakeClock{}
	var buf bytes.Buffer
	config := replay.Config{
		Speed:  1.0,
		Output: replay.OutputKeyOnly,
		Writer: &buf,
		Clock:  clock,
	}

	r := replay.NewReplayer(config)
	if err := r.Replay(events); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(clock.Sleeps) != 0 {
		t.Errorf("expected no sleeps for single event, got %d", len(clock.Sleeps))
	}
}

func TestReplayer_EmptyEvents(t *testing.T) {
	clock := &replay.FakeClock{}
	var buf bytes.Buffer
	config := replay.Config{
		Speed:  1.0,
		Output: replay.OutputKeyOnly,
		Writer: &buf,
		Clock:  clock,
	}

	r := replay.NewReplayer(config)
	if err := r.Replay(nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if buf.Len() != 0 {
		t.Errorf("expected no output for empty events, got %q", buf.String())
	}
}

func TestReplayer_OutputKeyOnly(t *testing.T) {
	t0 := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	events := []model.Event{
		makeEvent("order-101", "OrderCreated", t0),
	}

	var buf bytes.Buffer
	config := replay.Config{
		Speed:  0,
		Output: replay.OutputKeyOnly,
		Writer: &buf,
		Clock:  &replay.FakeClock{},
	}

	r := replay.NewReplayer(config)
	if err := r.Replay(events); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "order-101") {
		t.Errorf("expected output to contain key, got %q", output)
	}
	if !strings.Contains(output, "OrderCreated") {
		t.Errorf("expected output to contain event type, got %q", output)
	}
}

func TestReplayer_OutputSummary(t *testing.T) {
	t0 := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	events := []model.Event{
		makeEvent("order-101", "OrderCreated", t0),
	}

	var buf bytes.Buffer
	config := replay.Config{
		Speed:  0,
		Output: replay.OutputSummary,
		Writer: &buf,
		Clock:  &replay.FakeClock{},
	}

	r := replay.NewReplayer(config)
	if err := r.Replay(events); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "topic=") {
		t.Errorf("expected summary to contain topic=, got %q", output)
	}
	if !strings.Contains(output, "partition=") {
		t.Errorf("expected summary to contain partition=, got %q", output)
	}
}

func TestReplayer_OutputFull(t *testing.T) {
	t0 := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	events := []model.Event{
		makeEvent("order-101", "OrderCreated", t0),
	}

	var buf bytes.Buffer
	config := replay.Config{
		Speed:  0,
		Output: replay.OutputFull,
		Writer: &buf,
		Clock:  &replay.FakeClock{},
	}

	r := replay.NewReplayer(config)
	if err := r.Replay(events); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "Type:") {
		t.Errorf("expected full output to contain 'Type:', got %q", output)
	}
	if !strings.Contains(output, "Value:") {
		t.Errorf("expected full output to contain 'Value:', got %q", output)
	}
}
