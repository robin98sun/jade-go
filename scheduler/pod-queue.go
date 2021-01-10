package scheduler

import (
	"math"
	"sync"
	"time"
	"uta.edu/aces/jade-go/kernel"
)

type PodQueue struct {
	Pod          *kernel.Pod
	Queue        []*PodQueueItem
	ItemsInQueue map[string]*PodQueueItem
	mutex        *sync.Mutex
	dequeueClock int64
}

func NewPodQueue() *PodQueue {
	return &PodQueue{
		Queue:        []*PodQueueItem{},
		mutex:        &sync.Mutex{},
		ItemsInQueue: make(map[string]*PodQueueItem),
		dequeueClock: 0,
	}
}

func (p *PodQueue) Lock() {
	p.mutex.Lock()
}

func (p *PodQueue) Unlock() {
	p.mutex.Unlock()
}

func (p *PodQueue) Length() int {
	if p == nil {
		return 0
	}
	return len(p.Queue)
}

type PodQueueItem struct {
	Payload              interface{}
	TaskKey              string
	SubtaskKey           string
	ArrivalTime          time.Time
	Deadline             time.Time
	DispatchTime         time.Time
	Key                  string
	enqueueTime          int64
	dequeueTime          int64
	QueueLength          int64
	EstimatedServiceTime int64
}

func (q *PodQueue) Enqueue(
	key string, taskKey string, subtaskKey string, payload interface{},
	queueType kernel.TaskQueuingMechanism, maxQueuingTime int64,
	estimatedServiceTime int64, // milliseconds
) bool {
	if payload == nil || key == "" {
		return false
	}
	if _, e := q.ItemsInQueue[key]; e {
		return false
	}
	newItem := &PodQueueItem{
		Payload:              payload,
		ArrivalTime:          time.Now(),
		Key:                  key,
		TaskKey:              taskKey,
		SubtaskKey:           subtaskKey,
		enqueueTime:          q.dequeueClock,
		dequeueTime:          0,
		EstimatedServiceTime: estimatedServiceTime,
	}
	newItem.Deadline = newItem.ArrivalTime.Add(time.Duration(maxQueuingTime) * time.Millisecond)
	q.Lock()
	defer q.Unlock()

	if queueType == kernel.TaskQueuingFIFO || len(q.Queue) == 0 {
		q.Queue = append(q.Queue, newItem)
	} else if queueType == kernel.TaskQueuingDDL {
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
	q.ItemsInQueue[key] = newItem
	return true
}

func (q *PodQueue) Dequeue() *PodQueueItem {
	q.Lock()
	defer q.Unlock()
	if len(q.Queue) > 0 {
		item := q.Queue[0]
		q.Queue = q.Queue[1:]
		delete(q.ItemsInQueue, item.Key)
		item.dequeueTime = q.dequeueClock
		item.DispatchTime = time.Now()
		// calculate queue length
		if item.dequeueTime >= item.enqueueTime {
			item.QueueLength = item.dequeueTime - item.enqueueTime
		} else {
			item.QueueLength = math.MaxInt64 - item.enqueueTime + item.dequeueTime
		}
		// move dequeue clock
		if q.dequeueClock == math.MaxInt64 {
			q.dequeueClock = 1
		} else {
			q.dequeueClock++
		}

		return item
	}
	return nil
}
