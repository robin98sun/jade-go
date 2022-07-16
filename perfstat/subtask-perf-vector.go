package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	// "sync"
	"time"
)


type SubtaskPerfVector struct {
	DispatchItem 			 *scheduler.TaskDispatchingItem
	SubtaskPerf  			 map[string]*SubtaskPerfItem
	TailLatency  			 float64
	Fanout       			 int
	DeadlineViolationCount 	 int
	MaxDeadlineViolationTime float64
	CumulativeDeadlineViolationTime float64
}

func NewSubtaskPerfVector(
	dispatchItem *scheduler.TaskDispatchingItem, 
	subtasks map[string][]*scheduler.TaskCacheSubtaskItem,
	tailLatency float64,
) *SubtaskPerfVector {
	vector := &SubtaskPerfVector{
		DispatchItem: dispatchItem,
		SubtaskPerf: make(map[string]*SubtaskPerfItem),
		TailLatency: tailLatency,
	}

	for snKey, snItems := range subtasks {
		if len(snItems) == 0 {
			continue
		}

		perfItem := &SubtaskPerfItem{}

		for _, subtaskItem := range snItems {
			vector.Fanout += 1

			perfItem.ResponseTime += float64(float64(subtaskItem.RequestTime) / float64(time.Millisecond))
			perfItem.GivenBudget += subtaskItem.Budget
			perfItem.CommunicationTime += float64(float64(subtaskItem.CommunicationTime) / float64(time.Millisecond))
			perfItem.QueueingTime += float64(float64(subtaskItem.QueueingTime) / float64(time.Millisecond))

			// it's violation time, so it shall be negative or zero if not violated
			deadlineViolationTime := perfItem.QueueingTime - subtaskItem.Budget
			perfItem.DeadlineViolationTime += deadlineViolationTime
			if perfItem.QueueingTime > subtaskItem.Budget {
				vector.DeadlineViolationCount += 1	
				perfItem.DeadlineViolationCount += 1
			}
			if deadlineViolationTime > vector.MaxDeadlineViolationTime {
				vector.MaxDeadlineViolationTime = deadlineViolationTime
			}
			vector.CumulativeDeadlineViolationTime += deadlineViolationTime

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

		vector.SubtaskPerf[snKey] = perfItem

	}

	return vector
}