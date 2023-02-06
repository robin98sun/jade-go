package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	// "uta.edu/aces/jade-go/scheduler"
	"sync"
	// "time"
	"log"
)

type EventType string
const (
	EventTypeQueuePerformance EventType = "queue-perf"	
	EventTypeTaskPerformance EventType = "task-perf"
	EventTypeEnvPerformance EventType = "env-perf"
)

///////////////////////////////////////////////////////////////////////////
// Queue performance item
type QueuePerfItem struct {
	Hits                   int
	Success				   int
	Budget                 float64
	DeadlineViolationTime  float64
	DeadlineViolationRatio float64
	DeadlineSurplusRatio   float64
	ServiceResponseTime    float64
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
		Success: i.Success,
		DeadlineViolationTime: i.DeadlineViolationTime,
		DeadlineViolationRatio: i.DeadlineViolationRatio,
		DeadlineSurplusRatio: i.DeadlineSurplusRatio,
		ServiceResponseTime: i.ServiceResponseTime,
		DeadlineViolationCount: i.DeadlineViolationCount,
		MaximumResponseCount: i.MaximumResponseCount,
		ExceedingTaskSLOCount: i.ExceedingTaskSLOCount,
		MaximumAndExceedingTaskSLOCount: i.MaximumAndExceedingTaskSLOCount,
		QueueKey: i.QueueKey,
	}
}

func (i *QueuePerfItem) Add(j *QueuePerfItem) {
	if i.Hits + j.Hits == 0 {return}

	i.Success += j.Success
	i.Budget = (i.Budget*float64(i.Hits) + j.Budget*float64(j.Hits))/float64(i.Hits+j.Hits)
	i.DeadlineViolationTime = (i.DeadlineViolationTime * float64(i.Hits) + j.DeadlineViolationTime * float64(j.Hits)) / float64(i.Hits + j.Hits)
	i.DeadlineViolationRatio = (i.DeadlineViolationRatio*float64(i.Hits)+j.DeadlineViolationRatio*float64(j.Hits))/float64(i.Hits+j.Hits)
	i.DeadlineSurplusRatio = (i.DeadlineSurplusRatio*float64(i.Hits)+j.DeadlineSurplusRatio*float64(j.Hits))/float64(i.Hits+j.Hits)
	i.ServiceResponseTime = (i.ServiceResponseTime*float64(i.Hits) + j.ServiceResponseTime*float64(j.Hits))/float64(i.Hits+j.Hits)
	i.DeadlineViolationCount += j.DeadlineViolationCount
	i.MaximumResponseCount += j.MaximumResponseCount
	i.ExceedingTaskSLOCount += j.ExceedingTaskSLOCount
	i.MaximumAndExceedingTaskSLOCount += j.MaximumAndExceedingTaskSLOCount

	i.Hits += j.Hits
}

func (i *QueuePerfItem) Minus(j *QueuePerfItem) {
	
	i.Success -= j.Success

	if i.Hits-j.Hits == 0 {
		i.DeadlineViolationTime = 0
		i.ServiceResponseTime = 0
		i.DeadlineViolationRatio = 0
		i.DeadlineSurplusRatio = 0
		i.Budget = 0
	} else {
		i.DeadlineViolationTime = (i.DeadlineViolationTime*float64(i.Hits) - j.DeadlineViolationTime*float64(j.Hits))/float64(i.Hits - j.Hits)
		i.ServiceResponseTime = (i.ServiceResponseTime*float64(i.Hits) - j.ServiceResponseTime*float64(j.Hits))/float64(i.Hits-j.Hits)
		i.Budget = (i.Budget*float64(i.Hits)-j.Budget*float64(j.Hits))/float64(i.Hits - j.Hits)
		i.DeadlineViolationRatio = (i.DeadlineViolationRatio*float64(i.Hits)-j.DeadlineViolationRatio*float64(j.Hits))/float64(i.Hits-j.Hits)
		i.DeadlineSurplusRatio = (i.DeadlineSurplusRatio*float64(i.Hits)-j.DeadlineSurplusRatio*float64(j.Hits))/float64(i.Hits-j.Hits)
	}
	i.DeadlineViolationCount -= j.DeadlineViolationCount
	i.MaximumResponseCount -= j.MaximumResponseCount
	i.ExceedingTaskSLOCount -= j.ExceedingTaskSLOCount
	i.MaximumAndExceedingTaskSLOCount -= j.MaximumAndExceedingTaskSLOCount

	i.Hits -= j.Hits
}


///////////////////////////////////////////////////////////////////////////
// task performance item
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

	t.ResponseTime = (t.ResponseTime*float64(t.Count)+i.ResponseTime*float64(i.Count))/float64(t.Count+i.Count)

	t.Count += i.Count
}

func (t *TaskPerfItem) Minus(i *TaskPerfItem) {
	t.SLOViolationCount -= i.SLOViolationCount
	t.NormalizedSLOViolationCount -= i.NormalizedSLOViolationCount

	t.ResponseTime = (t.ResponseTime*float64(t.Count)-i.ResponseTime*float64(i.Count))/float64(t.Count-i.Count)

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

///////////////////////////////////////////////////////////////////////////
// environment performance item
type EnvPerfItem struct {
	CPUFrequence float64
	CPUTemperature float64
	CPUIdle float64
	SystemContextSwitches float64
	VoltageCore float64
	Count int64
	QueueKey string
}

func (i *EnvPerfItem) Add(j *EnvPerfItem) {

	if j.Count <= 0 {return}
	
	total := float64(i.Count + j.Count)

	i.CPUFrequence = (i.CPUFrequence * float64(i.Count) + j.CPUFrequence * float64(j.Count)) / total
	i.CPUTemperature = (i.CPUTemperature * float64(i.Count) + j.CPUTemperature * float64(j.Count)) / total
	i.CPUIdle = (i.CPUIdle * float64(i.Count) + j.CPUIdle * float64(j.Count)) / total
	i.SystemContextSwitches = (i.SystemContextSwitches * float64(i.Count) + j.SystemContextSwitches * float64(j.Count)) / total
	i.VoltageCore = (i.VoltageCore * float64(i.Count) + j.VoltageCore * float64(j.Count)) / total

	i.Count += j.Count

}

func (i *EnvPerfItem) Minus(j *EnvPerfItem) {
	total := float64(i.Count)
	i.Count -= j.Count
	
	if i.Count < 0 {
		i.Count = 0
	}

	if i.Count > 0 {
		i.CPUFrequence = (i.CPUFrequence * total - j.CPUFrequence * float64(j.Count)) / float64(i.Count)
		i.CPUTemperature = (i.CPUTemperature * total - j.CPUTemperature * float64(j.Count)) / float64(i.Count)
		i.CPUIdle = (i.CPUIdle * total - j.CPUIdle * float64(j.Count)) / float64(i.Count)
		i.SystemContextSwitches = (i.SystemContextSwitches * total - j.SystemContextSwitches * float64(j.Count)) / float64(i.Count)
		i.VoltageCore = (i.VoltageCore * total - j.VoltageCore * float64(j.Count)) / float64(i.Count)
	} else {
		i.CPUFrequence = 0
		i.CPUTemperature = 0
		i.CPUIdle = 0
		i.SystemContextSwitches = 0
		i.VoltageCore = 0
	}

}

func (i *EnvPerfItem) Copy() *EnvPerfItem {
	if i == nil {return nil}
	return &EnvPerfItem{
		CPUFrequence: i.CPUFrequence,
		CPUTemperature: i.CPUTemperature,
		CPUIdle: i.CPUIdle,
		SystemContextSwitches: i.SystemContextSwitches,
		VoltageCore: i.VoltageCore,
		Count: i.Count,
		QueueKey: i.QueueKey,
	}
}
///////////////////////////////////////////////////////////////////////////



type Event struct {
	EventType EventType
	QueuePerf *QueuePerfItem
	TaskPerf  *TaskPerfItem
	EnvPerf *EnvPerfItem
	Callback  *func(uint64)
}

type PerfEventVector struct {
	EventClock uint64
	Depth int
	ProcessingTime float64
	Interval   float64
	QueueSlice  map[string]*QueuePerfItem
	TaskClasses map[string]*TaskPerfItem
	InstantEnvPerf map[string]*EnvPerfItem
	AvgEnvPerf map[string]*EnvPerfItem
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
		InstantEnvPerf: make(map[string]*EnvPerfItem), 
		AvgEnvPerf: make(map[string]*EnvPerfItem), 
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

	if v.InstantEnvPerf != nil {
		for label, item := range v.InstantEnvPerf {
			newVector.InstantEnvPerf[label] = item.Copy()
		}	
	}

	if v.AvgEnvPerf != nil {
		for label, item := range v.AvgEnvPerf {
			newVector.AvgEnvPerf[label] = item.Copy()
		}	
	}

	return newVector
}

func (v *PerfEventVector) GetAverageTaskSLOViolationRatio(isViolation bool) float64 {
	if v == nil {
		return 0
	}

	v.mutex.Lock()
	defer v.mutex.Unlock()

	bar_R := float64(0)
	// total := v.TaskCount
	total := int64(0)
	for _, taskPerf := range v.TaskClasses {
		total += taskPerf.Count
		if total != v.TaskCount {
			log.Printf("WARNING: task count in the PerfEventVector does NOT equal with the sum of the tasks, real total: %v, task count: %v", total, v.TaskCount)
		}
	}
	for _, taskPerf := range v.TaskClasses {

		r := float64(taskPerf.SLOViolationCount)/float64(taskPerf.Count)

		if isViolation {
			r -= 1 - taskPerf.Percentile
		}
		if r > 0 {
			w := float64(taskPerf.Count) / float64(total)
			bar_R += r*w
			// log.Printf("clock: %v, slo: %v, pct: %v, r: %v, w: %v, barR: %v, cv: %v, ct: %v, total: %v, total in vector: %v",
			// 	v.EventClock, taskPerf.TailLatencySLO, taskPerf.Percentile, 
			// 	r, w, bar_R, taskPerf.SLOViolationCount, taskPerf.Count, total, v.TaskCount,
			// )
		}

	}
	return bar_R
}

func (v *PerfEventVector) GetAverageTaskSLOViolationThreshold() float64 {
	if v == nil {
		return 0
	}

	v.mutex.Lock()
	defer v.mutex.Unlock()

	bar_R := float64(0)
	// total := v.TaskCount
	total := float64(0)
	for _, taskPerf := range v.TaskClasses {
		total += float64(taskPerf.Count)
	}
	for _, taskPerf := range v.TaskClasses {

		r := 1 - taskPerf.Percentile

		w := float64(taskPerf.Count) / float64(total)
		bar_R += r*w
		// log.Printf("clock: %v, slo: %v, pct: %v, r: %v, w: %v, barR: %v, cv: %v, ct: %v, total: %v, total in vector: %v",
		// 	v.EventClock, taskPerf.TailLatencySLO, taskPerf.Percentile, 
		// 	r, w, bar_R, taskPerf.SLOViolationCount, taskPerf.Count, total, v.TaskCount,
		// )

	}
	return bar_R
}


func (v *PerfEventVector) Cumulate(incomingVector *PerfEventVector, outgoingVector *PerfEventVector) *PerfEventVector {

	v.EventClock = incomingVector.EventClock
	for queueKey, perfItem := range incomingVector.QueueSlice {
		if scalar, e := v.QueueSlice[queueKey]; e{
			scalar.Add(perfItem)
		} else {
			v.QueueSlice[queueKey] = perfItem.Copy()
		}
	}

	for label, taskPerf := range incomingVector.TaskClasses {
		if scalar, e := v.TaskClasses[label]; e{
			scalar.Add(taskPerf)
		} else {
			v.TaskClasses[label] = taskPerf.Copy()
		}
	}

	for label, envPerf := range incomingVector.InstantEnvPerf {
		if scalar, e := v.AvgEnvPerf[label]; e{
			scalar.Add(envPerf)
		} else {
			v.AvgEnvPerf[label] = envPerf.Copy()
		}
		v.InstantEnvPerf[label] = envPerf.Copy()
	}

	v.TaskSLOViolationCount += incomingVector.TaskSLOViolationCount
	v.NormalizedTaskSLOViolationCount += incomingVector.NormalizedTaskSLOViolationCount
	v.TaskCount += incomingVector.TaskCount
	v.Depth++

	if outgoingVector != nil{

		for label, item := range outgoingVector.QueueSlice {
			if scalar, e := v.QueueSlice[label]; e{
				scalar.Minus(item)
			} 
		}
		for label, item := range outgoingVector.TaskClasses {
			if scalar, e := v.TaskClasses[label]; e{
				scalar.Minus(item)
			}
		}
		for label, envPerf := range outgoingVector.InstantEnvPerf {
			if scalar, e := v.AvgEnvPerf[label]; e{
				scalar.Minus(envPerf)
			}
			v.InstantEnvPerf[label] = envPerf.Copy()
		}

		v.TaskSLOViolationCount -= outgoingVector.TaskSLOViolationCount
		v.NormalizedTaskSLOViolationCount -= outgoingVector.NormalizedTaskSLOViolationCount
		v.TaskCount -= outgoingVector.TaskCount
		v.Depth--

	}

	return v
}

