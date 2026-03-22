package parser

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// JSONArrayParser parses a JSON array of events.
type JSONArrayParser struct{}

func (p JSONArrayParser) Parse(r io.Reader) ([]model.Event, error) {
	var events []model.Event
	decoder := json.NewDecoder(r)

	if err := decoder.Decode(&events); err != nil {
		return nil, fmt.Errorf("parsing JSON array: %w", err)
	}

	return events, nil
}

// JSONLinesParser parses newline-delimited JSON (one event per line).
type JSONLinesParser struct{}

func (p JSONLinesParser) Parse(r io.Reader) ([]model.Event, error) {
	var events []model.Event
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var event model.Event
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, fmt.Errorf("parsing line %d: %w", lineNum, err)
		}
		events = append(events, event)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading lines: %w", err)
	}

	return events, nil
}
