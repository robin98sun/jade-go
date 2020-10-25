package kernel

import (
	"sync"
	"time"
)

type QueueType string

const (
	QueueTypeFIFO     QueueType = "fifo"
	QueueTypeDeadline           = "deadline"
)

type PodQueue struct {
	Cache map[string]*PodCacheItem // key: applicationID + subtaskType
	mutex *sync.Mutex
}

func NewPodQueue() *PodQueue {
	return &PodQueue{
		Cache: map[string]*PodCacheItem{},
		mutex: &sync.Mutex{},
	}
}

func (p *PodQueue) Lock() {
	p.mutex.Lock()
}

func (p *PodQueue) Unlock() {
	p.mutex.Unlock()
}

type PodCacheItem struct {
	Pod   *Pod
	Queue []*PodQueueItem
	mutex *sync.Mutex
}

func (i *PodCacheItem) Lock() {
	i.mutex.Lock()
}

func (i *PodCacheItem) Unlock() {
	i.mutex.Unlock()
}

type PodQueueItem struct {
	Payload     interface{}
	ArrivalTime time.Time
	Deadline    time.Time
	TimeToRun   int64 // in milliseconds
}

func (q *PodQueue) Set(pod *Pod) {
	if pod == nil {
		return
	}
	q.Lock()
	defer q.Unlock()
	key := pod.GetKey()
	if item, exists := q.Cache[key]; exists {
		item.Pod = pod
	} else {
		item = &PodCacheItem{
			Pod:   pod,
			Queue: []*PodQueueItem{},
			mutex: &sync.Mutex{},
		}
		q.Cache[key] = item
	}
}

func (q *PodQueue) Get(podkey string) *Pod {
	if q.Cache == nil {
		return nil
	}
	q.Lock()
	defer q.Unlock()
	if item, exists := q.Cache[podkey]; exists {
		return item.Pod
	}
	return nil
}

func (q *PodQueue) GetOrCreatePodForAppOnNode(appKey string, moduleName string, nodeKey string) *Pod {
	pod := q.Get(GenPodKey(appKey, moduleName, nodeKey))
	if pod == nil {
		pod = NewPod(appKey, moduleName, nodeKey)
		q.Set(pod)
	}
	return pod
}

func (q *PodQueue) Enqueue(podKey string, payload interface{}, queueType QueueType, timeToRun int64) bool {
	if podKey == "" || payload == nil {
		return false
	}
	if cacheItem, exists := q.Cache[podKey]; exists {
		newItem := &PodQueueItem{
			Payload:     payload,
			ArrivalTime: time.Now(),
			TimeToRun:   timeToRun,
		}
		newItem.Deadline = newItem.ArrivalTime.Add(time.Duration(timeToRun) * time.Millisecond)
		cacheItem.Lock()
		defer cacheItem.Unlock()
		if queueType == QueueTypeFIFO || len(cacheItem.Queue) == 0 {
			cacheItem.Queue = append(cacheItem.Queue, newItem)
		} else if queueType == QueueTypeDeadline {
			point := -1
			for i := 0; i < len(cacheItem.Queue); i++ {
				item := cacheItem.Queue[i]
				if item.Deadline.Sub(newItem.Deadline) <= 0 {
					continue
				} else {
					point = i
				}
			}
			if point < 0 {
				cacheItem.Queue = append(cacheItem.Queue, newItem)
			} else {
				newQueue := cacheItem.Queue[0:point]
				newQueue = append(newQueue, newItem)
				newQueue = append(newQueue, cacheItem.Queue[point:]...)
				cacheItem.Queue = newQueue
			}
		}
	}
	return false
}

func (q *PodQueue) Dequeue(podKey string, queueType QueueType) interface{} {
	if podKey == "" {
		return nil
	}
	if cacheItem, exists := q.Cache[podKey]; exists && len(cacheItem.Queue) > 0 {
		cacheItem.Lock()
		defer cacheItem.Unlock()
		item := cacheItem.Queue[0]
		cacheItem.Queue = cacheItem.Queue[1:]
		return item.Payload
	}
	return nil
}
