package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jade-go/kernel"
	"sync"
	"time"
)


type SubtaskPerfVector struct {
	ArrivalClock                    uint64
	ResponseClock					uint64
	DispatchItem 			 		*scheduler.TaskDispatchingItem
	SubtaskPerf  			 		map[string]*SubtaskPerfItem
	TailLatency  			 		float64
	Fanout       			 		int
	DeadlineViolationCount 	 		int
	MaxDeadlineViolationTime 		float64
	CumulativeDeadlineViolationTime float64
	UnloadedTailLatency      		float64
	QueueingBudget          		float64
	ProvisionOverhead               float64
	AggregationOverhead             float64
	AdjustedUnloadedTaillatency     float64
	DispatchingRate                 float64
	InstantOverallArrivalRateAtBeginning   float64
	InstantOverallArrivalRateAtEnd         float64
	InstantTaskArrivalRateAtBeginning   float64
	InstantTaskArrivalRateAtEnd         float64

	MostRecentCumulativePerfVectorAtBeginning *PerfEventVector
	MostRecentCumulativePerfVectorAtEnd  *PerfEventVector

	mutex *sync.Mutex

}

func NewSubtaskPerfVector(
	dispatchItem *scheduler.TaskDispatchingItem,
) *SubtaskPerfVector {
	vector := &SubtaskPerfVector{
		DispatchItem: dispatchItem,
		SubtaskPerf: make(map[string]*SubtaskPerfItem),
		mutex: &sync.Mutex{},
	}

	return vector
}

func (v *SubtaskPerfVector) GetTaskKey() string {
	if v != nil && v.DispatchItem != nil && v.DispatchItem.Task != nil {
		return v.DispatchItem.Task.GetKey()
	}
	return "N/A"
}

func (v *SubtaskPerfVector) IncarnateSubtasks(subtasks map[string][]*scheduler.TaskCacheSubtaskItem) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	for snKey, snItems := range subtasks {
		if len(snItems) == 0 {
			continue
		}

		perfItem := &SubtaskPerfItem{}

		for _, subtaskItem := range snItems {
			// only count for worker module
			if subtaskItem.GetModuleName() == string(kernel.AppModuleAggregator) {
				continue
			}
			v.Fanout += 1

			perfItem.ResponseTime += float64(float64(subtaskItem.RequestTime) / float64(time.Millisecond))
			perfItem.GivenBudget += subtaskItem.Budget
			perfItem.CommunicationTime += float64(float64(subtaskItem.CommunicationTime) / float64(time.Millisecond))
			perfItem.QueueingTime += float64(float64(subtaskItem.QueueingTime) / float64(time.Millisecond))

			// it's violation time, so it shall be negative or zero if not violated
			deadlineViolationTime := perfItem.QueueingTime - subtaskItem.Budget
			perfItem.DeadlineViolationTime += deadlineViolationTime
			if perfItem.QueueingTime > subtaskItem.Budget {
				v.DeadlineViolationCount += 1	
				perfItem.DeadlineViolationCount += 1
			}
			if deadlineViolationTime > v.MaxDeadlineViolationTime {
				v.MaxDeadlineViolationTime = deadlineViolationTime
			}
			v.CumulativeDeadlineViolationTime += deadlineViolationTime

		}

		// do average here (since M/M/1 only has 1 subtask per node)
		// but could do other stat if wanted

		if len(snItems) > 1 {
			perfItem.ResponseTime /= float64(len(snItems))
			perfItem.GivenBudget /= float64(len(snItems))
			perfItem.CommunicationTime /= float64(len(snItems))
			perfItem.QueueingTime /= float64(len(snItems))
			perfItem.DeadlineViolationTime /= float64(len(snItems))
		}

		v.SubtaskPerf[snKey] = perfItem

	}
}

func (v *SubtaskPerfVector) GetEventsOfStrugglingQueues(taskLatencySLO float64, provisionOverhead float64, aggregationOverhead float64) []*Event {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	events := []*Event{}

	max_key := ""
	max_response_time := float64(0)
	event_dict := map[string]*Event{}
	for key, item := range v.SubtaskPerf {
		
		if item.ResponseTime + item.QueueingTime > taskLatencySLO - provisionOverhead - aggregationOverhead {
			event := &Event{
				EventType: EventTypeQueuePerformance,
				QueuePerf: &QueuePerfItem{
					QueueKey: key,
					ExceedingTaskSLOCount: 1,
				},
			}
			event_dict[key] = event
			events = append(events, event)
		}
		if item.ResponseTime > max_response_time {
			max_response_time = item.ResponseTime
			max_key = key
		}
	}

	if item, e := event_dict[max_key]; e {
		item.QueuePerf.MaximumResponseCount = 1
		if item.QueuePerf.ExceedingTaskSLOCount > 0 {
			item.QueuePerf.MaximumAndExceedingTaskSLOCount = 1
		}
	} else {
		event := &Event{
			EventType: EventTypeQueuePerformance,
			QueuePerf: &QueuePerfItem{
				MaximumResponseCount: 1,
				QueueKey: max_key,
			},
		}
		events = append(events, event)
	}

	return events
} 
