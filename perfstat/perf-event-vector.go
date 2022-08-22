package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	// "uta.edu/aces/jade-go/scheduler"
	"sync"
	// "time"
)

type EventType string
const (
	EventTypeQueuePerformance EventType = "queue-perf"	
	EventTypeTaskPerformance EventType = "task-perf"
)

type QueuePerfItem struct {
	Hits                   int
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
		Hits: i.Hits,
		DeadlineViolationTime: i.DeadlineViolationTime,
		DeadlineViolationCount: i.DeadlineViolationCount,
		MaximumResponseCount: i.MaximumResponseCount,
		ExceedingTaskSLOCount: i.ExceedingTaskSLOCount,
		MaximumAndExceedingTaskSLOCount: i.MaximumAndExceedingTaskSLOCount,
		QueueKey: i.QueueKey,
	}
}

func (i *QueuePerfItem) Add(j *QueuePerfItem) {
	i.Hits += j.Hits
	i.DeadlineViolationTime += j.DeadlineViolationTime
	i.DeadlineViolationCount += j.DeadlineViolationCount
	i.MaximumResponseCount += j.MaximumResponseCount
	i.ExceedingTaskSLOCount += j.ExceedingTaskSLOCount
	i.MaximumAndExceedingTaskSLOCount += j.MaximumAndExceedingTaskSLOCount
}

func (i *QueuePerfItem) Minus(j *QueuePerfItem) {
	i.Hits -= j.Hits
	i.DeadlineViolationTime -= j.DeadlineViolationTime
	i.DeadlineViolationCount -= j.DeadlineViolationCount
	i.MaximumResponseCount -= j.MaximumResponseCount
	i.ExceedingTaskSLOCount -= j.ExceedingTaskSLOCount
	i.MaximumAndExceedingTaskSLOCount -= j.MaximumAndExceedingTaskSLOCount
}

type TaskPerfItem struct {
	TailLatencySLO float64
	Percentile float64
	ResponseTime float64
	SLOViolationCount int64
	NormalizedSLOViolationCount float64
	Count int64
}

func (t *TaskPerfItem) Add(i *TaskPerfItem) {
	t.SLOViolationCount += i.SLOViolationCount
	t.NormalizedSLOViolationCount += i.NormalizedSLOViolationCount
	t.Count += i.Count
}

func (t *TaskPerfItem) Minus(i *TaskPerfItem) {
	t.SLOViolationCount -= i.SLOViolationCount
	t.NormalizedSLOViolationCount -= i.NormalizedSLOViolationCount
	t.Count -= i.Count
}

func (t *TaskPerfItem) Copy() *TaskPerfItem {
	if t == nil {return nil}

	return &TaskPerfItem{
		TailLatencySLO: t.TailLatencySLO,
		Percentile: t.Percentile,
		ResponseTime: t.ResponseTime,
		SLOViolationCount: t.SLOViolationCount,
		NormalizedSLOViolationCount: t.NormalizedSLOViolationCount,
		Count: t.Count,
	}
}

type Event struct {
	EventType EventType
	QueuePerf *QueuePerfItem
	TaskPerf  *TaskPerfItem
	Callback  *func(uint64)
}

type PerfEventVector struct {
	EventClock uint64
	Depth int
	ProcessingTime float64
	Interval   float64
	QueueSlice  map[string]*QueuePerfItem
	TaskClasses map[string]*TaskPerfItem
	TaskSLOViolationCount int64
	NormalizedTaskSLOViolationCount float64
	TaskCount int64
	mutex *sync.Mutex
}

func NewPerfEventVector() *PerfEventVector {
	return &PerfEventVector{
		EventClock: 0,
		Depth: 0,
		ProcessingTime: 0,
		Interval: 0,
		QueueSlice: make(map[string]*QueuePerfItem),
		TaskClasses: make(map[string]*TaskPerfItem),
		TaskSLOViolationCount: 0,
		NormalizedTaskSLOViolationCount: 0,
		TaskCount: 0,
		mutex: &sync.Mutex{},
	}
}

func (v *PerfEventVector) Copy() *PerfEventVector {
	if v == nil {
		return nil
	}
	v.mutex.Lock()
	defer v.mutex.Unlock()

	newVector := NewPerfEventVector()
	newVector.EventClock = v.EventClock
	newVector.Depth = v.Depth
	newVector.ProcessingTime = v.ProcessingTime
	newVector.Interval = v.Interval
	newVector.TaskSLOViolationCount = v.TaskSLOViolationCount
	newVector.NormalizedTaskSLOViolationCount = v.NormalizedTaskSLOViolationCount
	newVector.TaskCount = v.TaskCount

	if v.QueueSlice != nil {
		for queueKey, item := range v.QueueSlice {
			newVector.QueueSlice[queueKey] = item.Copy()
		}		
	}

	if v.TaskClasses != nil {
		for label, item := range v.TaskClasses {
			newVector.TaskClasses[label] = item.Copy()
		}		
	}

	return newVector
}

func (v *PerfEventVector) GetAverageTaskSLOViolationRatio() float64 {
	if v == nil {
		return 0
	}

	v.mutex.Lock()
	defer v.mutex.Unlock()

	bar_R := float64(0)
	total := v.TaskCount

	for _, taskPerf := range v.TaskClasses {
		r := float64(taskPerf.SLOViolationCount)/float64(taskPerf.Count) - taskPerf.TailLatencySLO
		w := float64(taskPerf.Count) / float64(total)
		bar_R += r*w
	}
	return bar_R
}

