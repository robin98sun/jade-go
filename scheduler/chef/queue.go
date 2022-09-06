package chef

import (
	"math"
	"sync"
	"time"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler/histogram"
)

type QueueType string

const (
	QueueTypeMain 	QueueType  = "main"
	QueueTypeShadow  QueueType  = "shadow"
)

type QueueHistogramType string
const(
	QueueHistogramTypeServiceResponseTime QueueHistogramType = "service-response-time"
	QueueHistogramTypeServiceResponseTimeWithQueueingTime QueueHistogramType = "service-response-time-with-queueing-time"
	QueueHistogramTypeAdjustedServiceResponseTime QueueHistogramType = "adjusted-service-response-time"
)

type Queue struct {
	MainQueue       []*QueueItem
	ShadowQueue  	[]*QueueItem
	ItemsInQueue 	map[string]*QueueItem
	mutex        *sync.Mutex
	HistogramServiceTime *histogram.Histogram
	HistogramWithQueueingTime *histogram.Histogram
	HistogramAdjustedServiceTime *histogram.Histogram
	// HistogramInQueueTime *histogram.Histogram
	// HistogramCommunicationTime *histogram.Histogram
	dequeueClock int64
	Key string
}

func NewQueue(key string, hist_length int) *Queue {
	h_st := histogram.NewHistogram(int64(hist_length), float64(0.1), 1)
	h_wq := histogram.NewHistogram(int64(hist_length), float64(0.1), 1)
	h_ad := histogram.NewHistogram(int64(hist_length), float64(0.1), 1)
	return &Queue{
		MainQueue:        []*QueueItem{},
		ShadowQueue:  	  []*QueueItem{},
		mutex:        	  &sync.Mutex{},
		ItemsInQueue: 	  make(map[string]*QueueItem),
		HistogramServiceTime:  		  h_st,
		HistogramWithQueueingTime:    h_wq,
		HistogramAdjustedServiceTime: h_ad,
		dequeueClock: 0,
		Key: key,
	}
}

func (p *Queue) Lock() {
	p.mutex.Lock()
}

func (p *Queue) Unlock() {
	p.mutex.Unlock()
}


func (p *Queue) Clean() {
	p.Lock()
	defer p.Unlock()

	p.dequeueClock = 0
	p.MainQueue = []*QueueItem{}
	p.ShadowQueue = []*QueueItem{}
	p.ItemsInQueue = make(map[string]*QueueItem)
}

func (p *Queue) Length() int {
	if p == nil {
		return 0
	}
	return len(p.MainQueue)
}

type QueueItem struct {
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
	MinimumCapacityRequirement *kernel.Capacity
}

func (q *Queue) search_insertion_place(low int, high int, ddl time.Time, pri int) int {
	qlen := len(q.MainQueue)
	if pri >= 0 {
		if pri >= q.MainQueue[qlen-1].Priority {
			return -1
		} else if pri < q.MainQueue[0].Priority {
			return 0
		}
	} else if !ddl.IsZero() {
		if ddl.Sub(q.MainQueue[qlen-1].Deadline) >= 0 {
			return -1
		} else if ddl.Sub(q.MainQueue[0].Deadline) < 0 {
			return 0
		}
	}
	
	if low >= high {
		target := low
		if high >=0 {
			target = high
		}
		if pri >= 0 {
			if pri == q.MainQueue[target].Priority{
				target += 1
			}
		} else if !ddl.IsZero() {
			if ddl.Sub(q.MainQueue[target].Deadline) == 0 {
				target += 1
			}
		}
		return target
	}
	median := int((low+high)/2)
	if pri >=0 {
		if pri >= q.MainQueue[median].Priority {
			return q.search_insertion_place(median+1, high, ddl, pri)
		} else {
			return q.search_insertion_place(low, median-1, ddl, pri)
		}
	} else if !ddl.IsZero() {
		if ddl.Sub(q.MainQueue[median].Deadline) >= 0 {
			return q.search_insertion_place(median+1, high, ddl, pri)
		} else {
			return q.search_insertion_place(low, median-1, ddl, pri)
		}
	}
	return -1
}

func (q *Queue) Enqueue( 
	queueType QueueType,
	queueItemKey string, taskKey string, subtaskKey string, payload interface{},
	queueingMechanism kernel.TaskQueuingMechanism, maxQueuingTime float64, priority int,
	estimatedServiceTime float64, // milliseconds
	minCapReq *kernel.Capacity,
	printf func(string, ...interface{}),
) (bool, *QueueItem, int) {
	result := false
	if payload == nil || queueItemKey == "" {
		return result, nil, 0
	}
	q.Lock()
	defer q.Unlock()

	theQueue := q.MainQueue
	if queueType == QueueTypeShadow {
		theQueue = q.ShadowQueue
	}
	if printf != nil {
		printf("[queue] enqueuing to [%v] queue", queueType)
	}

	if queueingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock &&
	   queueType == QueueTypeMain {
		if _, e := q.ItemsInQueue[queueItemKey]; e {
			// find the item in shadow queue
			// and delete it from shadow queue
			newShadowQueue := []*QueueItem{}
			for _, item := range q.ShadowQueue {
				if item.Key != queueItemKey {
					newShadowQueue = append(newShadowQueue, item)
				}
			}
			q.ShadowQueue = newShadowQueue
			printf("[pod queue] the task still exists in the %v queue, now move it to the %v queue", QueueTypeShadow, QueueTypeMain)
			// but no need to delete from cache, since it will be updated anyway
			// delete(q.ItemsInQueue, key)
		} else {
			// otherwise, it means the item has been dispatched already
			// quit directly
			printf("[pod queue] the task has been dispatched in the %v queue", QueueTypeShadow)
			return result, nil, 0
		}
	} else {
		if _, e := q.ItemsInQueue[queueItemKey]; e {
			printf("[pod queue] the task already in the %v queue", queueType)
			return result, nil, 0
		}
	}
	newItem := &QueueItem{
		Payload:              payload,
		ArrivalTime:          time.Now(),
		Key:                  queueItemKey,
		TaskKey:              taskKey,
		SubtaskKey:           subtaskKey,
		enqueueTime:          q.dequeueClock,
		dequeueTime:          0,
		EstimatedServiceTime: estimatedServiceTime,
		EnqueuingOverhead:    time.Duration(0),
		AmountPreempted:      0,
		Budget:               maxQueuingTime,
		Priority:             priority,
		MinimumCapacityRequirement: minCapReq,
	}
	enqueueStart := time.Now()
	index_in_queue := len(theQueue)

	newItem.Deadline = newItem.ArrivalTime.Add(time.Duration(maxQueuingTime) * time.Millisecond)

	if printf != nil {
		printf("[queue][%v] an item is enqueuing at the queue clock %v, there are %v items in queue and %v in cache right now",
			q.Key,
			newItem.enqueueTime,
			len(q.MainQueue) + len(q.ShadowQueue),
			len(q.ItemsInQueue),
		)
	}
	if queueingMechanism == kernel.TaskQueuingFIFO {
		if printf != nil {
			printf("[pod queue][%v] enqueuing the new item using FIFO Queuing, queueingMechanism: %v", q.Key, queueingMechanism)
		}
		theQueue = append(theQueue, newItem)
	} else if 	queueingMechanism == kernel.TaskQueuingDDL || 
				queueingMechanism == kernel.TaskQueuingPRQ ||
				queueingMechanism == kernel.TaskQueuingClass ||
				queueingMechanism == kernel.TaskQueuingDDL_CDF_Block ||
				queueingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock ||
				queueingMechanism == kernel.TaskQueuingDDL_None  {
		if printf != nil {
			printf("[queue][%v] enqueuing the new item using queueingMechanism: %v, budget: %v, priority: %v", q.Key, queueingMechanism, newItem.Budget, newItem.Priority)
		}
		qlen := len(theQueue)
		if qlen == 0 || queueType == QueueTypeShadow{
			theQueue = append(theQueue, newItem)
			if printf != nil {
				if queueType == QueueTypeShadow {
					printf("[queue][%v] enqueued the new item at the end of the %v queue as FIFO, queueingMechanism: %v", q.Key, queueType, queueingMechanism)
				} else {
					printf("[queue][%v] enqueued the new item at the end of the %v queue as FIFO because the queue is empty, queueingMechanism: %v", q.Key, queueType, queueingMechanism)

				}
			}
		} else {
			point := -1
			if queueingMechanism == kernel.TaskQueuingPRQ && newItem.Priority >= 0 {
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
			} else {
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
			}
			
			// got the position to insert the task
			if point < 0 {
				theQueue = append(theQueue, newItem)
				if printf != nil {
					printf("[queue][%v] enqueued the new item at the end of the queue like FIFO because reaching the end of the queue, queueingMechanism: %v", q.Key, queueingMechanism)
				}
			} else {
				// newQueue := q.Queue[0:point]
				// newQueue = append(newQueue, newItem)
				// newQueue = append(newQueue, q.Queue[point:]...)
				originalLength := len(theQueue)
				newQueue := []*QueueItem{}
				for i:=0; i<point; i++ {
					newQueue = append(newQueue, theQueue[i])
				}
				newQueue = append(newQueue, newItem)
				for i:=point; i<len(theQueue); i++ {
					newQueue = append(newQueue, theQueue[i])
				}
				theQueue = newQueue
				if printf != nil {
					printf("[queue][%v] enqueued the new item at the index {%v} of the queue in front of {%v} existing items, queueingMechanism: %v", 
						q.Key, 
						point, 
						originalLength - point, 
						queueingMechanism,
					)
				}
				newItem.AmountPreempted = originalLength - point
				index_in_queue = point
			}
		}
	} 
	q.ItemsInQueue[queueItemKey] = newItem
	if printf != nil {
		printf("[queue][%v] the item is enqueued at the queue clock %v, there are %v items in queue and %v in cache right now",
			q.Key,
			newItem.enqueueTime,
			len(theQueue),
			len(q.ItemsInQueue),
		)
	}
	
	result = true
	enqueueEnd := time.Now()
	newItem.EnqueuingOverhead = enqueueEnd.Sub(enqueueStart)

	if queueType == QueueTypeMain {
		q.MainQueue = theQueue
	} else if queueType == QueueTypeShadow {
		q.ShadowQueue = theQueue
	}

	if len(q.MainQueue) + len(q.ShadowQueue) != len(q.ItemsInQueue) && printf != nil {
		printf("[queue][%v][ERROR] [%v] items in queue not equal with [%v] items in cache",
			q.Key, len(theQueue), len(q.ItemsInQueue),
		)
	}

	return result, newItem, index_in_queue
}

func (q *Queue) Dequeue(printf func(string, ...interface{})) *QueueItem {
	q.Lock()
	defer q.Unlock()
	var item *QueueItem

	targetQueue := QueueTypeMain
	if len(q.MainQueue) > 0 {
		item = q.MainQueue[0]
		q.MainQueue = q.MainQueue[1:]
	} else if len(q.ShadowQueue) > 0 {
		item = q.ShadowQueue[0]
		q.ShadowQueue = q.ShadowQueue[1:]
		targetQueue = QueueTypeShadow
	}
	if item != nil {
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
			printf("[queue][%v] dequeued an item from [%v] at queue clock %v, which waited %v previous items, and queueing time is %v",
				q.Key, targetQueue,
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
			printf("[queue][%v] after dequeuing the item from [%v], the queue clock changed to %v, and there are %v items in queue right now",
				q.Key, targetQueue,
				q.dequeueClock,
				len(q.MainQueue) + len(q.ShadowQueue),
			)
		}
	}
	return item
}
