package tests

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func binaryPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "kafka-replay")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/kafka-replay")
	cmd.Dir = projectRoot()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("building binary: %v\n%s", err, out)
	}
	return bin
}

func projectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filename))
}

func testdata(parts ...string) string {
	args := append([]string{projectRoot(), "testdata"}, parts...)
	return filepath.Join(args...)
}

func run(t *testing.T, bin string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = projectRoot()
	out, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			t.Fatalf("running command: %v", err)
		}
	}
	return string(out), exitCode
}

// --- Replay ---

func TestIntegration_Replay_KeyOnly(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "replay", testdata("events", "simple.json"), "--speed", "0", "--output", "key-only")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if !strings.Contains(out, "order-101") {
		t.Errorf("expected output to contain order-101: %s", out)
	}
	if !strings.Contains(out, "Replaying 5 events") {
		t.Errorf("expected replay header: %s", out)
	}
}

func TestIntegration_Replay_JSONL(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "replay", testdata("events", "simple.jsonl"), "--speed", "0", "--output", "summary")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if !strings.Contains(out, "topic=") {
		t.Errorf("expected summary output: %s", out)
	}
}

func TestIntegration_Replay_MissingFile(t *testing.T) {
	bin := binaryPath(t)
	_, code := run(t, bin, "replay", "/nonexistent/file.json", "--speed", "0")
	if code == 0 {
		t.Fatal("expected non-zero exit for missing file")
	}
}

// --- Filter ---

func TestIntegration_Filter_ByType(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "filter", testdata("events", "simple.json"), "--type", "OrderCreated", "--count")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if strings.TrimSpace(out) != "2" {
		t.Errorf("expected count 2, got %q", strings.TrimSpace(out))
	}
}

func TestIntegration_Filter_ByKey(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "filter", testdata("events", "simple.json"), "--key", "order-101")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if !strings.Contains(out, "order-101") {
		t.Errorf("expected output to contain order-101: %s", out)
	}
}

func TestIntegration_Filter_OutputFile(t *testing.T) {
	bin := binaryPath(t)
	outFile := filepath.Join(t.TempDir(), "filtered.json")
	out, code := run(t, bin, "filter", testdata("events", "simple.json"), "--type", "OrderCreated", "--output-file", outFile)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("reading output file: %v", err)
	}
	if !strings.Contains(string(data), "OrderCreated") {
		t.Errorf("expected filtered output to contain OrderCreated")
	}
}

func TestIntegration_Filter_FieldFilter(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "filter", testdata("events", "simple.json"),
		"--field", "status=confirmed", "--count")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if strings.TrimSpace(out) != "1" {
		t.Errorf("expected count 1, got %q", strings.TrimSpace(out))
	}
}

// --- Stats ---

func TestIntegration_Stats_Table(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "stats", testdata("events", "simple.json"))
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if !strings.Contains(out, "Total Events:") {
		t.Errorf("expected stats summary: %s", out)
	}
	if !strings.Contains(out, "OrderCreated") {
		t.Errorf("expected event type counts: %s", out)
	}
}

func TestIntegration_Stats_JSON(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "stats", testdata("events", "simple.json"), "-f", "json")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
	if result["TotalEvents"] != float64(5) {
		t.Errorf("expected TotalEvents=5, got %v", result["TotalEvents"])
	}
}

func TestIntegration_Stats_LargeBatch(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "stats", testdata("events", "large_batch.jsonl"))
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if !strings.Contains(out, "1000") {
		t.Errorf("expected 1000 total events: %s", out)
	}
}

// --- Diff ---

func TestIntegration_Diff_Table(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "diff",
		testdata("diff", "stream_a.json"), testdata("diff", "stream_b.json"))
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if !strings.Contains(out, "MODIFIED") {
		t.Errorf("expected MODIFIED in diff output: %s", out)
	}
	if !strings.Contains(out, "ADDED") {
		t.Errorf("expected ADDED in diff output: %s", out)
	}
	if !strings.Contains(out, "REMOVED") {
		t.Errorf("expected REMOVED in diff output: %s", out)
	}
}

func TestIntegration_Diff_JSON(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "diff",
		testdata("diff", "stream_a.json"), testdata("diff", "stream_b.json"),
		"-f", "json")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
}

func TestIntegration_Diff_Summary(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "diff",
		testdata("diff", "stream_a.json"), testdata("diff", "stream_b.json"),
		"--summary")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	// Summary mode should NOT show individual entries
	if strings.Contains(out, "MODIFIED") {
		t.Errorf("summary mode should not show individual entries: %s", out)
	}
	if !strings.Contains(out, "Diff Summary") {
		t.Errorf("expected diff summary header: %s", out)
	}
}

func TestIntegration_Diff_IdenticalFiles(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "diff",
		testdata("events", "simple.json"), testdata("events", "simple.json"))
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if !strings.Contains(out, "Unchanged:") {
		t.Errorf("expected unchanged count: %s", out)
	}
}

// --- Validate ---

func TestIntegration_Validate_Valid(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "validate", testdata("events", "simple.json"),
		"--schema", testdata("schemas", "order_event.schema.json"),
		"--type", "OrderCreated")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if !strings.Contains(out, "2 valid") {
		t.Errorf("expected 2 valid: %s", out)
	}
}

func TestIntegration_Validate_Invalid(t *testing.T) {
	bin := binaryPath(t)
	_, code := run(t, bin, "validate", testdata("events", "simple.json"),
		"--schema", testdata("schemas", "order_event.schema.json"))
	// Should fail because PaymentProcessed doesn't match order schema
	if code == 0 {
		t.Fatal("expected non-zero exit for validation failure")
	}
}

func TestIntegration_Validate_JSON(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "validate", testdata("events", "simple.json"),
		"--schema", testdata("schemas", "order_event.schema.json"),
		"--type", "OrderCreated", "-f", "json")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
}

func TestIntegration_Validate_MissingSchema(t *testing.T) {
	bin := binaryPath(t)
	_, code := run(t, bin, "validate", testdata("events", "simple.json"),
		"--schema", "/nonexistent/schema.json")
	if code == 0 {
		t.Fatal("expected non-zero exit for missing schema")
	}
}

// --- DLQ Inspect ---

func TestIntegration_DLQInspect_Table(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "dlq-inspect", testdata("dlq", "dlq_sample.json"))
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if !strings.Contains(out, "DLQ Summary") {
		t.Errorf("expected DLQ summary: %s", out)
	}
	if !strings.Contains(out, "schema_validation_failed") {
		t.Errorf("expected failure reason: %s", out)
	}
}

func TestIntegration_DLQInspect_JSON(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "dlq-inspect", testdata("dlq", "dlq_sample.json"), "-f", "json")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
}

func TestIntegration_DLQInspect_GroupByKey(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin, "dlq-inspect", testdata("dlq", "dlq_sample.json"),
		"--group-by", "key")
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, out)
	}
	if !strings.Contains(out, "order-101") {
		t.Errorf("expected order-101 in key grouping: %s", out)
	}
}

// --- Error cases ---

func TestIntegration_NoArgs(t *testing.T) {
	bin := binaryPath(t)
	out, code := run(t, bin)
	if code != 0 {
		t.Fatalf("expected exit 0 for help, got %d", code)
	}
	if !strings.Contains(out, "Available Commands:") {
		t.Errorf("expected help output: %s", out)
	}
}

func TestIntegration_InvalidFormat(t *testing.T) {
	bin := binaryPath(t)
	_, code := run(t, bin, "stats", testdata("events", "simple.json"), "-f", "xml")
	if code == 0 {
		t.Fatal("expected non-zero exit for invalid format")
	}
}

func TestIntegration_ReplayInvalidOutput(t *testing.T) {
	bin := binaryPath(t)
	_, code := run(t, bin, "replay", testdata("events", "simple.json"),
		"--speed", "0", "--output", "invalid")
	if code == 0 {
		t.Fatal("expected non-zero exit for invalid output mode")
	}
}
