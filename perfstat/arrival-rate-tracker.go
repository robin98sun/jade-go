package perfstat

import (
	"time"
)

type ArrivalRateTracker struct {
	Length int
	Queue []time.Time
}

func NewArrivalRateTracker(length int) *ArrivalRateTracker {
	return &ArrivalRateTracker{
		Length: length,
	}
}

func (a *ArrivalRateTracker) Enqueue(arrivalTime time.Time) time.Time {

	var dequeuedTime time.Time

	if a.Queue == nil {
		a.Queue = []time.Time{}
	}
	a.Queue = append(a.Queue, arrivalTime)
	if len(a.Queue) > a.Length {
		dequeuedTime = a.Dequeue()
	}
	return dequeuedTime
}

func (a *ArrivalRateTracker) Dequeue() time.Time {
	var dequeuedTime time.Time

	if len(a.Queue) > 0 {
		dequeuedTime = a.Queue[0]
		a.Queue = a.Queue[1:]
	}

	return dequeuedTime
}

func (a *ArrivalRateTracker) GetArrivalRatePerSecond() float64 {
	if len(a.Queue) <= 1 {
		return 0
	}

	timespanInSeconds := float64(a.Queue[len(a.Queue)-1].Sub(a.Queue[0])/time.Second)

	if timespanInSeconds == 0 {
		return 0
	}

	return float64(len(a.Queue)) / timespanInSeconds
}
