package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	// "uta.edu/aces/jade-go/scheduler"
	// "sync"
	// "time"
)

type EventType string
const (
	EventTypeQueuePerformance EventType = "queue-perf"	
	EventTypeTaskPerformance EventType = "task-perf"
)

type QueuePerfItem struct {
	DeadlineViolationTime  float64
	DeadlineViolationCount int
	QueueKey               string
}

func (i *QueuePerfItem) Copy() *QueuePerfItem {
	if i == nil{return nil}

	return &QueuePerfItem{
		DeadlineViolationTime: i.DeadlineViolationTime,
		DeadlineViolationCount: i.DeadlineViolationCount,
		QueueKey: i.QueueKey,
	}
}

type TaskPerfItem struct {
	TailLatencySLO float64
	Percentile float64
	ResponseTime float64
	SLOViolationCount int
	NormalizedSLOViolationCount float64
}

type Event struct {
	EventType EventType
	QueuePerf *QueuePerfItem
	TaskPerf  *TaskPerfItem
}

type PerfEventVector struct {
	EventClock uint64
	QueueSlice  map[string]*QueuePerfItem
	TaskSLOViolationCount int
	NormalizedTaskSLOViolationCount float64
}

func (v *PerfEventVector) Copy() *PerfEventVector {
	if v == nil {
		return nil
	}
	newVector := &PerfEventVector{
		EventClock: v.EventClock,
		TaskSLOViolationCount: v.TaskSLOViolationCount,
		NormalizedTaskSLOViolationCount: v.NormalizedTaskSLOViolationCount,
	}

	if v.QueueSlice != nil {
		newVector.QueueSlice = map[string]*QueuePerfItem{}
		for queueKey, item := range v.QueueSlice {
			newVector.QueueSlice[queueKey] = item.Copy()
		}		
	}

	return newVector
}

