package parser

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/linkedin/goavro/v2"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// AvroParser parses Avro OCF (Object Container Format) files.
type AvroParser struct{}

func (p AvroParser) Parse(r io.Reader) ([]model.Event, error) {
	ocfReader, err := goavro.NewOCFReader(r)
	if err != nil {
		return nil, fmt.Errorf("opening Avro OCF: %w", err)
	}

	var events []model.Event
	for ocfReader.Scan() {
		datum, err := ocfReader.Read()
		if err != nil {
			return nil, fmt.Errorf("reading Avro record: %w", err)
		}

		event, err := avroRecordToEvent(datum)
		if err != nil {
			return nil, fmt.Errorf("converting Avro record: %w", err)
		}
		events = append(events, event)
	}

	if err := ocfReader.Err(); err != nil {
		return nil, fmt.Errorf("Avro reader error: %w", err)
	}

	return events, nil
}

func avroRecordToEvent(datum any) (model.Event, error) {
	record, ok := datum.(map[string]any)
	if !ok {
		return model.Event{}, fmt.Errorf("expected map record, got %T", datum)
	}

	topic := avroString(record, "topic")
	partition := avroInt(record, "partition")
	offset := avroInt64(record, "offset")
	key := avroString(record, "key")
	eventType := avroString(record, "event_type")
	ts := avroTimestamp(record, "timestamp")

	headers := avroHeaders(record, "headers")

	value := avroValue(record, "value")

	return model.NewEvent(topic, partition, offset, key, value, headers, ts, eventType), nil
}

func avroString(record map[string]any, key string) string {
	v, ok := record[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	return s
}

func avroInt(record map[string]any, key string) int {
	v, ok := record[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func avroInt64(record map[string]any, key string) int64 {
	v, ok := record[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func avroTimestamp(record map[string]any, key string) time.Time {
	v, ok := record[key]
	if !ok {
		return time.Time{}
	}
	switch t := v.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339, t)
		if err != nil {
			return time.Time{}
		}
		return parsed
	case int64:
		return time.UnixMilli(t).UTC()
	case float64:
		return time.UnixMilli(int64(t)).UTC()
	default:
		return time.Time{}
	}
}

func avroHeaders(record map[string]any, key string) map[string]string {
	v, ok := record[key]
	if !ok {
		return nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]string, len(m))
	for k, val := range m {
		result[k] = fmt.Sprintf("%v", val)
	}
	return result
}

func avroValue(record map[string]any, key string) json.RawMessage {
	v, ok := record[key]
	if !ok {
		return json.RawMessage(`{}`)
	}

	switch val := v.(type) {
	case string:
		if json.Valid([]byte(val)) {
			return json.RawMessage(val)
		}
		raw, _ := json.Marshal(val)
		return raw
	case map[string]any:
		raw, err := json.Marshal(val)
		if err != nil {
			return json.RawMessage(`{}`)
		}
		return raw
	default:
		raw, err := json.Marshal(val)
		if err != nil {
			return json.RawMessage(`{}`)
		}
		return raw
	}
}
