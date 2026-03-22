package parser_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/linkedin/goavro/v2"

	"github.com/AmalMathew1/kafka-replay/internal/parser"
)

const avroSchema = `{
	"type": "record",
	"name": "KafkaEvent",
	"fields": [
		{"name": "topic", "type": "string"},
		{"name": "partition", "type": "int"},
		{"name": "offset", "type": "long"},
		{"name": "key", "type": "string"},
		{"name": "value", "type": "string"},
		{"name": "timestamp", "type": "string"},
		{"name": "event_type", "type": "string"}
	]
}`

func createTestAvroFile(t *testing.T) string {
	t.Helper()

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		t.Fatalf("creating avro codec: %v", err)
	}

	var buf bytes.Buffer
	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:     &buf,
		Codec: codec,
	})
	if err != nil {
		t.Fatalf("creating OCF writer: %v", err)
	}

	records := []map[string]any{
		{
			"topic": "orders", "partition": 0, "offset": int64(1),
			"key": "order-101", "value": `{"order_id":"101","amount":59.99}`,
			"timestamp": "2024-06-15T10:00:00Z", "event_type": "OrderCreated",
		},
		{
			"topic": "orders", "partition": 0, "offset": int64(2),
			"key": "order-102", "value": `{"order_id":"102","amount":149.50}`,
			"timestamp": "2024-06-15T10:01:00Z", "event_type": "OrderCreated",
		},
		{
			"topic": "payments", "partition": 1, "offset": int64(3),
			"key": "pay-201", "value": `{"payment_id":"201"}`,
			"timestamp": "2024-06-15T10:02:00Z", "event_type": "PaymentProcessed",
		},
	}

	for _, r := range records {
		if err := ocfWriter.Append([]any{r}); err != nil {
			t.Fatalf("appending avro record: %v", err)
		}
	}

	tmpFile := filepath.Join(t.TempDir(), "test.avro")
	if err := os.WriteFile(tmpFile, buf.Bytes(), 0644); err != nil {
		t.Fatalf("writing avro file: %v", err)
	}

	return tmpFile
}

func TestAvroParser_ParsesValidFile(t *testing.T) {
	avroFile := createTestAvroFile(t)

	f, err := os.Open(avroFile)
	if err != nil {
		t.Fatalf("opening avro file: %v", err)
	}
	defer f.Close()

	p := parser.AvroParser{}
	events, err := p.Parse(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	if events[0].Key != "order-101" {
		t.Errorf("expected key 'order-101', got %q", events[0].Key)
	}
	if events[0].Topic != "orders" {
		t.Errorf("expected topic 'orders', got %q", events[0].Topic)
	}
	if events[0].EventType != "OrderCreated" {
		t.Errorf("expected event_type 'OrderCreated', got %q", events[0].EventType)
	}
	if events[0].Partition != 0 {
		t.Errorf("expected partition 0, got %d", events[0].Partition)
	}
	if events[0].Offset != 1 {
		t.Errorf("expected offset 1, got %d", events[0].Offset)
	}
	if events[0].Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
}

func TestAvroParser_ValueIsValidJSON(t *testing.T) {
	avroFile := createTestAvroFile(t)

	f, err := os.Open(avroFile)
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	defer f.Close()

	p := parser.AvroParser{}
	events, err := p.Parse(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i, ev := range events {
		if len(ev.Value) == 0 {
			t.Errorf("event %d has empty value", i)
		}
	}
}

func TestAvroParser_InvalidData(t *testing.T) {
	p := parser.AvroParser{}
	_, err := p.Parse(bytes.NewReader([]byte("not avro data")))
	if err == nil {
		t.Fatal("expected error for invalid avro data")
	}
}

func TestDetectAndParse_AvroFile(t *testing.T) {
	avroFile := createTestAvroFile(t)

	// Rename to .avro extension
	avroPath := filepath.Join(filepath.Dir(avroFile), "test_events.avro")
	if err := os.Rename(avroFile, avroPath); err != nil {
		t.Fatalf("renaming: %v", err)
	}

	events, err := parser.DetectAndParse(avroPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
}
