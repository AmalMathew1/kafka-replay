package validate

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/AmalMathew1/kafka-replay/internal/model"
)

// ValidationError describes a single event that failed validation.
type ValidationError struct {
	Index     int
	Event     model.Event
	Errors    []string
}

// ValidationResult holds the outcome of validating a stream of events.
type ValidationResult struct {
	TotalEvents  int
	ValidCount   int
	InvalidCount int
	Errors       []ValidationError
}

// ValidateEvents validates each event's Value against the given JSON Schema file.
// Returns all validation errors unless strict is true, in which case it stops at the first.
func ValidateEvents(events []model.Event, schemaPath string, strict bool) (ValidationResult, error) {
	schema, err := compileSchema(schemaPath)
	if err != nil {
		return ValidationResult{}, fmt.Errorf("compiling schema %s: %w", schemaPath, err)
	}

	result := ValidationResult{TotalEvents: len(events)}

	for i, event := range events {
		var value any
		if err := json.Unmarshal(event.Value, &value); err != nil {
			ve := ValidationError{
				Index:  i,
				Event:  event,
				Errors: []string{fmt.Sprintf("invalid JSON in value: %v", err)},
			}
			result.Errors = append(result.Errors, ve)
			result.InvalidCount++
			if strict {
				return result, nil
			}
			continue
		}

		err := schema.Validate(value)
		if err != nil {
			ve := ValidationError{
				Index:  i,
				Event:  event,
				Errors: extractErrors(err),
			}
			result.Errors = append(result.Errors, ve)
			result.InvalidCount++
			if strict {
				return result, nil
			}
		} else {
			result.ValidCount++
		}
	}

	return result, nil
}

func compileSchema(path string) (*jsonschema.Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading schema file: %w", err)
	}

	var schemaDoc any
	if err := json.Unmarshal(data, &schemaDoc); err != nil {
		return nil, fmt.Errorf("parsing schema JSON: %w", err)
	}

	c := jsonschema.NewCompiler()
	if err := c.AddResource(path, schemaDoc); err != nil {
		return nil, fmt.Errorf("adding schema resource: %w", err)
	}

	return c.Compile(path)
}

func extractErrors(err error) []string {
	if ve, ok := err.(*jsonschema.ValidationError); ok {
		return flattenValidationErrors(ve, nil)
	}
	return []string{err.Error()}
}

func flattenValidationErrors(ve *jsonschema.ValidationError, result []string) []string {
	if ve.ErrorKind != nil {
		loc := "/" + joinPath(ve.InstanceLocation)
		msg := fmt.Sprintf("%s: %v", loc, ve.ErrorKind)
		result = append(result, msg)
	}
	for _, cause := range ve.Causes {
		result = flattenValidationErrors(cause, result)
	}
	return result
}

func joinPath(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	s := parts[0]
	for _, p := range parts[1:] {
		s += "/" + p
	}
	return s
}
