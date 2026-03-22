//go:build ignore

package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"
)

type event struct {
	Topic     string            `json:"topic"`
	Partition int               `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     json.RawMessage   `json:"value"`
	Headers   map[string]string `json:"headers"`
	Timestamp time.Time         `json:"timestamp"`
	EventType string            `json:"event_type"`
}

func main() {
	rng := rand.New(rand.NewSource(42))
	types := []string{"OrderCreated", "OrderConfirmed", "OrderCancelled", "PaymentProcessed", "ShipmentDispatched"}
	topics := []string{"orders", "orders", "orders", "payments", "shipments"}

	f, err := os.Create("testdata/events/large_batch.jsonl")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ts := time.Date(2024, 6, 15, 8, 0, 0, 0, time.UTC)
	offset := int64(1)

	for i := 0; i < 1000; i++ {
		typeIdx := rng.Intn(len(types))
		keyNum := rng.Intn(50) + 1
		partition := rng.Intn(4)

		// Normal gap: 1-15 seconds
		gap := time.Duration(rng.Intn(15)+1) * time.Second

		// Inject 5-minute gaps at specific positions
		if i == 200 || i == 600 {
			gap = 5*time.Minute + time.Duration(rng.Intn(60))*time.Second
		}

		ts = ts.Add(gap)

		// Inject duplicates at specific positions
		dupOffset := offset
		if i == 300 || i == 301 || i == 700 || i == 701 {
			dupOffset = 500
		}

		e := event{
			Topic:     topics[typeIdx],
			Partition: partition,
			Offset:    dupOffset,
			Key:       fmt.Sprintf("key-%03d", keyNum),
			Value:     json.RawMessage(fmt.Sprintf(`{"id":"%d","amount":%.2f}`, i, float64(rng.Intn(10000))/100)),
			Headers:   map[string]string{"source": "generator"},
			Timestamp: ts,
			EventType: types[typeIdx],
		}

		data, _ := json.Marshal(e)
		fmt.Fprintln(f, string(data))
		offset++
	}
	fmt.Println("Generated 1000 events to testdata/events/large_batch.jsonl")
}
