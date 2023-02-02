package perfstat

import(
	"sync"
	"time"
	"uta.edu/aces/jade-go/scheduler"
	ds "uta.edu/aces/jadesdk/data_structure"
)

type TaskCategoriesCache struct {
	mutex *sync.Mutex 

	MaximumTaskAmount int
	DaemonIntervalInMilliseconds int
	HistoryTimeWindowSize int

	TaskCategories map[string]*TaskCategoryItem

	TaskHead *TaskPerfVector
	TaskTail  *TaskPerfVector
	TaskCount int
	TaskSLOExceedingCount int
}

func NewTaskCategoriesCache() *TaskCategoriesCache {

	return &TaskCategoriesCache{
		mutex: &sync.Mutex{},
		TaskCategories: make(map[string]*TaskCategoryItem),
	}
}

func (p *TaskCategoriesCache) Clear() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.TaskCategories = make(map[string]*TaskCategoryItem)
	p.TaskHead = nil
	p.TaskTail = nil
	p.TaskCount = 0
	p.TaskSLOExceedingCount = 0
}

func (p *TaskCategoriesCache) SetMaximumTaskAmount(maximumTaskAmount int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.MaximumTaskAmount = maximumTaskAmount
}

func (p *TaskCategoriesCache) SetIterationTimeScaleInMilliseconds(timeScale int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.DaemonIntervalInMilliseconds = timeScale
}

func (p *TaskCategoriesCache) EnqueueArrivalTime(dispatchItem *ds.TaskDispatchingItem, arrivalTime time.Time, arrivalClock uint64, instantOverallArrivalRate float64, instantCumulativePerfVector *PerfEventVector) {

	taskTag := dispatchItem.GetUnifiedTag()

	p.mutex.Lock()
	if _, e := p.TaskCategories[taskTag]; !e {
		percentile := dispatchItem.GetPercentile()
		slo := dispatchItem.GetTailLatencySLOInMilliseconds()
		p.TaskCategories[taskTag] = NewTaskCategoryItem(percentile, slo)
	}
	categoryItem := p.TaskCategories[taskTag]
	p.mutex.Unlock()
	

	// arrivalClock := p.PerfEventMatrices.GetEventClock()

	// _, instantOverallArrivalRate := p.ArrivalRateTracker.Enqueue(arrivalTime)

	// instantCumulativePerfVector := p.PerfEventMatrices.GetInstantCumulativePerfVector()

	taskVector := categoryItem.ReserveForResponse(arrivalClock, dispatchItem, arrivalTime, instantOverallArrivalRate, instantCumulativePerfVector)


	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.TaskHead != nil {
		taskVector.After = p.TaskHead
		p.TaskHead.Before = taskVector
	}
	p.TaskHead = taskVector
	p.TaskCount++

	if p.TaskTail == nil {
		p.TaskTail = taskVector
	} else if p.MaximumTaskAmount > 0 && p.TaskCount > p.MaximumTaskAmount {
		// for p.TaskCount 
	}
}


func (p *TaskCategoriesCache) EnqueueResponse(dispatchItem *ds.TaskDispatchingItem, unloaded_tail_latency float64, queueing_budget float64, provision_overhead float64, aggregation_overhead float64, adjusted_unloaded_tail_latency float64, taskResponseTime float64, subtasks map[string][]*scheduler.TaskCacheSubtaskItem, instantOverallArrivalRate float64, instantCumulativePerfVector *PerfEventVector) *TaskPerfVector {

	taskTag := dispatchItem.GetUnifiedTag()

	p.mutex.Lock()
	if _, e := p.TaskCategories[taskTag]; !e {
		percentile := dispatchItem.GetPercentile()
		slo := dispatchItem.GetTailLatencySLOInMilliseconds()
		p.TaskCategories[taskTag] = NewTaskCategoryItem(percentile, slo)
	}
	categoryItem := p.TaskCategories[taskTag]
	p.mutex.Unlock()

	perfVector := categoryItem.EnqueueResponse(dispatchItem, taskResponseTime, unloaded_tail_latency, queueing_budget, provision_overhead, aggregation_overhead, adjusted_unloaded_tail_latency, subtasks, instantOverallArrivalRate, instantCumulativePerfVector)

	return perfVector

}



