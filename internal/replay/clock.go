package replay

import "time"

// Clock abstracts time operations for testability.
type Clock interface {
	Sleep(d time.Duration)
}

// RealClock uses actual time.Sleep.
type RealClock struct{}

func (RealClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

// FakeClock records sleep calls without actually sleeping.
type FakeClock struct {
	Sleeps []time.Duration
}

func (c *FakeClock) Sleep(d time.Duration) {
	c.Sleeps = append(c.Sleeps, d)
}
