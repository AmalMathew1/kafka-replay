package parser

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// DetectAndParse opens a file, detects its format, and parses events.
func DetectAndParse(filePath string) ([]model.Event, error) {
	p, err := DetectParser(filePath)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening file %s: %w", filePath, err)
	}
	defer f.Close()

	return p.Parse(f)
}

// DetectParser returns the appropriate parser for a file based on extension and content.
func DetectParser(filePath string) (EventParser, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".jsonl":
		return JSONLinesParser{}, nil
	case ".json":
		return detectJSONFormat(filePath)
	default:
		return detectJSONFormat(filePath)
	}
}

func detectJSONFormat(filePath string) (EventParser, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening file for detection %s: %w", filePath, err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("detecting format of %s: %w", filePath, err)
		}
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		if b == '[' {
			return JSONArrayParser{}, nil
		}
		return JSONLinesParser{}, nil
	}
}
