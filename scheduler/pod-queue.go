package scheduler

import (
	"math"
	"sync"
	"time"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/histogram"
)

type PodQueue struct {
	Pod          *kernel.Pod
	Queue        []*PodQueueItem
	ItemsInQueue map[string]*PodQueueItem
	mutex        *sync.Mutex
	HistogramServiceTime *histogram.Histogram
	// HistogramInQueueTime *histogram.Histogram
	// HistogramCommunicationTime *histogram.Histogram
	dequeueClock int64
}

func NewPodQueue() *PodQueue {
	h_st := histogram.NewHistogram(10000, float64(0.1), 1)
	h_st.AddPercentilePoint(float64(0.99))
	// h_qt := histogram.NewHistogram(10000, float64(0.1), 1)
	// h_qt.AddPercentilePoint(float64(0.99))
	// h_ct := histogram.NewHistogram(10000, float64(0.1), 1)
	// h_ct.AddPercentilePoint(float64(0.99))
	return &PodQueue{
		Queue:        []*PodQueueItem{},
		mutex:        &sync.Mutex{},
		ItemsInQueue: make(map[string]*PodQueueItem),
		HistogramServiceTime:  		h_st,
		// HistogramInQueueTime:  		h_qt,
		// HistogramCommunicationTime: h_ct,
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
	EstimatedServiceTime float64
	EnqueuingOverhead    time.Duration
	AmountPreempted      int
	Budget               float64
	Priority             int
}

func (q *PodQueue) search_insertion_place(low int, high int, ddl time.Time, pri int) int {
	qlen := len(q.Queue)
	if pri >= 0 {
		if pri >= q.Queue[qlen-1].Priority {
			return -1
		} else if pri < q.Queue[0].Priority {
			return 0
		}
	} else if !ddl.IsZero() {
		if ddl.Sub(q.Queue[qlen-1].Deadline) >= 0 {
			return -1
		} else if ddl.Sub(q.Queue[0].Deadline) < 0 {
			return 0
		}
	}
	
	if low >= high {
		target := low
		if high >=0 {
			target = high
		}
		if pri >= 0 {
			if pri == q.Queue[target].Priority{
				target += 1
			}
		} else if !ddl.IsZero() {
			if ddl.Sub(q.Queue[target].Deadline) == 0 {
				target += 1
			}
		}
		return target
	}
	median := int((low+high)/2)
	if pri >=0 {
		if pri >= q.Queue[median].Priority {
			return q.search_insertion_place(median+1, high, ddl, pri)
		} else {
			return q.search_insertion_place(low, median-1, ddl, pri)
		}
	} else if !ddl.IsZero() {
		if ddl.Sub(q.Queue[median].Deadline) >= 0 {
			return q.search_insertion_place(median+1, high, ddl, pri)
		} else {
			return q.search_insertion_place(low, median-1, ddl, pri)
		}
	}
	return -1
}

func (q *PodQueue) Enqueue(
	key string, taskKey string, subtaskKey string, payload interface{},
	queueType kernel.TaskQueuingMechanism, maxQueuingTime float64, priority int,
	estimatedServiceTime float64, // milliseconds
	printf func(string, ...interface{}),
) (bool, *PodQueueItem, int) {
	result := false
	if payload == nil || key == "" {
		return result, nil, 0
	}
	q.Lock()
	defer q.Unlock()
	
	if _, e := q.ItemsInQueue[key]; e {
		return result, nil, 0
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
		EnqueuingOverhead:      time.Duration(0),
		AmountPreempted:      0,
		Budget:               maxQueuingTime,
		Priority:             priority,
	}
	enqueueStart := time.Now()
	index_in_queue := len(q.Queue)

	newItem.Deadline = newItem.ArrivalTime.Add(time.Duration(maxQueuingTime) * time.Millisecond)
	podKey := "PodKey=N/A"
	if q.Pod != nil {
		podKey = q.Pod.GetKey()
	}
	if printf != nil {
		printf("[pod queue][%v] an item is enqueuing at the queue clock %v, there are %v items in queue and %v in cache right now",
			podKey,
			newItem.enqueueTime,
			len(q.Queue),
			len(q.ItemsInQueue),
		)
	}
	if queueType == kernel.TaskQueuingFIFO {
		if printf != nil {
			printf("[pod queue][%v] enqueuing the new item using FIFO Queuing, queueType: %v", podKey, queueType)
		}
		q.Queue = append(q.Queue, newItem)
	} else if queueType == kernel.TaskQueuingDDL || queueType == kernel.TaskQueuingPRQ || queueType == kernel.TaskQueuingClass {
		if printf != nil {
			printf("[pod queue][%v] enqueuing the new item using queueType: %v, budget: %v, priority: %v", podKey, queueType, newItem.Budget, newItem.Priority)
		}
		qlen := len(q.Queue)
		if qlen == 0 {
			q.Queue = append(q.Queue, newItem)
			if printf != nil {
				printf("[pod queue][%v] enqueued the new item at the end of the queue like FIFO because the queue is empty, queueType: %v", podKey, queueType)
			}
		} else {
			point := -1
			if queueType == kernel.TaskQueuingDDL || queueType == kernel.TaskQueuingClass {
				point = q.search_insertion_place(0, qlen, newItem.Deadline, -1)

				// this is for the sanity check, to use the most simplest formation
				// however, most simplest is the easist to be ignored when reviewing
				// for i := 0; i < len(q.Queue); i++ {
				// 	item := q.Queue[i]
				// 	if item.Deadline.Sub(newItem.Deadline) <= 0 {
				// 		continue
				// 	} else {
				// 		point = i
				// 		// the folloing piece of "break" had been forgotten for months 
				// 		// that ruined the whole damn experiments
				// 		// whenever and no matter how stringent the timeline is
				// 		// it's critical to write a unit test!
				// 		// that could save months of hard work from becoming non-sense!
				// 		break
				// 	}
				// }
			} else if queueType == kernel.TaskQueuingPRQ && newItem.Priority >= 0 {
				point = q.search_insertion_place(0, qlen, newItem.Deadline, newItem.Priority)

				// this is for the sanity check, to use the most simplest formation
				// however, most simplest is the easist to be ignored when reviewing
				// for i := 0; i<len(q.Queue); i++ {
				// 	item := q.Queue[i]
				// 	if item.Priority > newItem.Priority {
				// 		point = i
				// 		break
				// 	}
				// }
			}
			
			// got the position to insert the task
			if point < 0 {
				q.Queue = append(q.Queue, newItem)
				if printf != nil {
					printf("[pod queue][%v] enqueued the new item at the end of the queue like FIFO because reaching the end of the queue, queueType: %v", podKey, queueType)
				}
			} else {
				// newQueue := q.Queue[0:point]
				// newQueue = append(newQueue, newItem)
				// newQueue = append(newQueue, q.Queue[point:]...)
				originalLength := len(q.Queue)
				newQueue := []*PodQueueItem{}
				for i:=0; i<point; i++ {
					newQueue = append(newQueue, q.Queue[i])
				}
				newQueue = append(newQueue, newItem)
				for i:=point; i<len(q.Queue); i++ {
					newQueue = append(newQueue, q.Queue[i])
				}
				q.Queue = newQueue
				if printf != nil {
					printf("[pod queue][%v] enqueued the new item at the index {%v} of the queue in front of {%v} existing items, queueType: %v", 
						podKey, 
						point, 
						originalLength - point, 
						queueType,
					)
				}
				newItem.AmountPreempted = originalLength - point
				index_in_queue = point
			}
		}
	} 
	q.ItemsInQueue[key] = newItem
	if printf != nil {
		printf("[pod queue][%v] the item is enqueued at the queue clock %v, there are %v items in queue and %v in cache right now",
			podKey,
			newItem.enqueueTime,
			len(q.Queue),
			len(q.ItemsInQueue),
		)
	}
	if len(q.Queue) != len(q.ItemsInQueue) && printf != nil {
		printf("[pod queue][ERROR] [%v] items in queue not equal with [%v] items in cache",
			len(q.Queue), len(q.ItemsInQueue),
		)
	}
	result = true
	enqueueEnd := time.Now()
	newItem.EnqueuingOverhead = enqueueEnd.Sub(enqueueStart)
	return result, newItem, index_in_queue
}

func (q *PodQueue) Dequeue(printf func(string, ...interface{})) *PodQueueItem {
	q.Lock()
	defer q.Unlock()
	if len(q.Queue) > 0 {
		podKey := "PodKey=N/A"
		if q.Pod != nil {
			podKey = q.Pod.GetKey()
		}
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
		if printf != nil {
			printf("[pod queue][%v] dequeued an item at queue clock %v, which waited %v previous items, and queueing time is %v",
				podKey,
				q.dequeueClock,
				item.QueueLength,
				item.DispatchTime.Sub(item.ArrivalTime)/time.Millisecond,
			)
		}
		// move dequeue clock
		if q.dequeueClock == math.MaxInt64 {
			q.dequeueClock = 1
		} else {
			q.dequeueClock++
		}
		if printf != nil {
			printf("[pod queue][%v] after dequeuing the item, the queue clock changed to %v, and there are %v items in queue right now",
				podKey,
				q.dequeueClock,
				len(q.Queue),
			)
		}
		return item
	}
	return nil
}
