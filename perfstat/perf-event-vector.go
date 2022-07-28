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
	MaximumResponseCount   int
	ExceedingTaskSLOCount  int
	MaximumAndExceedingTaskSLOCount  int
	QueueKey               string
}

func (i *QueuePerfItem) Copy() *QueuePerfItem {
	if i == nil{return nil}

	return &QueuePerfItem{
		DeadlineViolationTime: i.DeadlineViolationTime,
		DeadlineViolationCount: i.DeadlineViolationCount,
		MaximumResponseCount: i.MaximumResponseCount,
		ExceedingTaskSLOCount: i.ExceedingTaskSLOCount,
		MaximumAndExceedingTaskSLOCount: i.MaximumAndExceedingTaskSLOCount,
		QueueKey: i.QueueKey,
	}
}

func (i *QueuePerfItem) Add(j *QueuePerfItem) {
	i.DeadlineViolationTime += j.DeadlineViolationTime
	i.DeadlineViolationCount += j.DeadlineViolationCount
	i.MaximumResponseCount += j.MaximumResponseCount
	i.ExceedingTaskSLOCount += j.ExceedingTaskSLOCount
	i.MaximumAndExceedingTaskSLOCount += j.MaximumAndExceedingTaskSLOCount
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
	Callback  *func(uint64)
}

type PerfEventVector struct {
	EventClock uint64
	ProcessingTime float64
	Interval   float64
	QueueSlice  map[string]*QueuePerfItem
	TaskSLOViolationCount int
	NormalizedTaskSLOViolationCount float64
}

func NewPerfEventVector() *PerfEventVector {
	return &PerfEventVector{
		EventClock: 0,
		QueueSlice: make(map[string]*QueuePerfItem),
		TaskSLOViolationCount: 0,
		NormalizedTaskSLOViolationCount: 0,
	}
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

