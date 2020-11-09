package scheduler

import (
	"aces/jade-go/kernel"
	"sync"
	"time"
)

type QueueType string

const (
	QueueTypeFIFO     QueueType = "fifo"
	QueueTypeDeadline           = "deadline"
)

type PodQueue struct {
	Pod   *kernel.Pod
	Queue []*PodQueueItem
	mutex *sync.Mutex
}

func NewPodQueue() *PodQueue {
	return &PodQueue{
		Queue: []*PodQueueItem{},
		mutex: &sync.Mutex{},
	}
}

func (p *PodQueue) Lock() {
	p.mutex.Lock()
}

func (p *PodQueue) Unlock() {
	p.mutex.Unlock()
}

type PodQueueItem struct {
	Payload     interface{}
	ArrivalTime time.Time
	Deadline    time.Time
	TimeToRun   int64 // in milliseconds
}

func (q *PodQueue) Enqueue(payload interface{}, queueType QueueType, timeToRun int64) bool {
	if payload == nil {
		return false
	}
	newItem := &PodQueueItem{
		Payload:     payload,
		ArrivalTime: time.Now(),
		TimeToRun:   timeToRun,
	}
	newItem.Deadline = newItem.ArrivalTime.Add(time.Duration(timeToRun) * time.Millisecond)
	q.Lock()
	defer q.Unlock()
	if queueType == QueueTypeFIFO || len(q.Queue) == 0 {
		q.Queue = append(q.Queue, newItem)
	} else if queueType == QueueTypeDeadline {
		point := -1
		for i := 0; i < len(q.Queue); i++ {
			item := q.Queue[i]
			if item.Deadline.Sub(newItem.Deadline) <= 0 {
				continue
			} else {
				point = i
			}
		}
		if point < 0 {
			q.Queue = append(q.Queue, newItem)
		} else {
			newQueue := q.Queue[0:point]
			newQueue = append(newQueue, newItem)
			newQueue = append(newQueue, q.Queue[point:]...)
			q.Queue = newQueue
		}
	}
	return false
}

func (q *PodQueue) Dequeue(podKey string, queueType QueueType) interface{} {
	if podKey == "" {
		return nil
	}
	q.Lock()
	defer q.Unlock()
	if len(q.Queue) > 0 {
		item := q.Queue[0]
		q.Queue = q.Queue[1:]
		return item.Payload
	}
	return nil
}
