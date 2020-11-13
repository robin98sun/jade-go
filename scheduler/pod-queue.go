package scheduler

import (
	"log"
	"sync"
	"time"
	"uta.edu/aces/jade-go/kernel"
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

func (p *PodQueue) Length() int {
	if p == nil {
		return 0
	}
	return len(p.Queue)
}

type PodQueueItem struct {
	Payload     interface{}
	ArrivalTime time.Time
	Deadline    time.Time
	TimeToRun   int64 // in milliseconds
}

func (q *PodQueue) Enqueue(payload interface{}, queueType kernel.TaskQueuingMechanism, timeToRun int64) bool {
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
	return false
}

func (q *PodQueue) Dequeue() interface{} {
	q.Lock()
	defer q.Unlock()
	log.Printf("dequeuing for pod{%v}", q.Pod.GetKey())
	if len(q.Queue) > 0 {
		item := q.Queue[0]
		q.Queue = q.Queue[1:]
		log.Println("dequeued an item:", item)
		log.Println("payload of the dequeued an item:", item.Payload)
		return item.Payload
	}
	log.Printf("queue of pod{%v} is empty", q.Pod.GetKey())
	return nil
}
