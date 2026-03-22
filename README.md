# kafka-replay

A CLI tool for replaying, filtering, diffing, validating, and analyzing Kafka event streams from exported files.

**No live Kafka connection required.** Works entirely with exported event files (JSON, JSONL, Avro). Clone the repo and run it immediately — no Docker, no Kafka cluster, no Zookeeper.

## Install

```bash
go install github.com/AmalMathew1/kafka-replay/cmd/kafka-replay@latest
```

Or build from source:

```bash
git clone https://github.com/AmalMathew1/kafka-replay.git
cd kafka-replay
make build
```

## Commands

### replay

Walk through events in timestamp order with timing simulation.

```bash
# Replay at original speed
kafka-replay replay events.json

# 2x speed, summary output
kafka-replay replay events.json --speed 2 --output summary

# No delay, key-only output
kafka-replay replay events.json --speed 0 --output key-only
```

### filter

Filter events by type, timestamp, key, or custom JSON field.

```bash
# By event type
kafka-replay filter events.json --type OrderCreated

# By key and time range
kafka-replay filter events.json --key order-101 --from 2024-06-15T10:00:00Z --to 2024-06-15T11:00:00Z

# By nested JSON field
kafka-replay filter events.json --field status=confirmed

# Count only
kafka-replay filter events.json --type OrderCreated --count

# Write to file
kafka-replay filter events.json --type OrderCreated --output-file filtered.json
```

### stats

Show event counts, throughput patterns, gaps, and duplicates.

```bash
kafka-replay stats events.json

# Custom window and gap threshold
kafka-replay stats events.json --window 5m --detect-gaps 10m

# JSON output for scripting
kafka-replay stats events.json -f json
```

### diff

Compare two event streams side by side.

```bash
# Diff by key (default)
kafka-replay diff stream_a.json stream_b.json

# Diff by partition+offset
kafka-replay diff stream_a.json stream_b.json --align-by offset

# Ignore specific fields
kafka-replay diff stream_a.json stream_b.json --ignore timestamp,headers

# Summary only
kafka-replay diff stream_a.json stream_b.json --summary
```

### validate

Check events against a JSON Schema.

```bash
# Validate all events
kafka-replay validate events.json --schema order_event.schema.json

# Validate only specific type
kafka-replay validate events.json --schema order_event.schema.json --type OrderCreated

# Strict mode (fail on first error)
kafka-replay validate events.json --schema order_event.schema.json --strict
```

### dlq-inspect

Analyze dead letter queue dumps for failure patterns.

```bash
# Group by failure reason (default)
kafka-replay dlq-inspect dlq_dump.json

# Group by key to find poison pills
kafka-replay dlq-inspect dlq_dump.json --group-by key

# Group by original topic
kafka-replay dlq-inspect dlq_dump.json --group-by topic

# Show individual events per pattern
kafka-replay dlq-inspect dlq_dump.json --show-events
```

## Global Flags

```
-f, --format string   Output format: table, json, plain (default "table")
```

All commands support `--format json` for machine-readable output, useful for piping to `jq`.

## Event File Format

Events are expected as JSON objects with these fields:

```json
{
  "topic": "orders",
  "partition": 0,
  "offset": 1,
  "key": "order-101",
  "value": {"order_id": "101", "amount": 59.99, "status": "created"},
  "headers": {"source": "order-service"},
  "timestamp": "2024-06-15T10:00:00Z",
  "event_type": "OrderCreated"
}
```

Supported formats:
- **JSON array** (`.json`): `[{event}, {event}, ...]`
- **JSON lines** (`.jsonl`): one event per line
- **Avro OCF** (`.avro`): Avro Object Container Format

## DLQ Entry Format

```json
{
  "event": { ... },
  "failure_reason": "schema_validation_failed",
  "original_topic": "orders",
  "retry_count": 3,
  "first_failed_at": "2024-06-15T09:58:00Z",
  "last_failed_at": "2024-06-15T10:00:00Z",
  "error_details": {"field": "amount", "message": "negative value"}
}
```

## Development

```bash
make test          # Run all tests
make test-unit     # Unit tests only
make test-integration  # Integration tests only
make test-cover    # Tests with coverage report
make lint          # Run go vet
make fmt           # Format code
make build         # Build binary
```

## License

MIT
