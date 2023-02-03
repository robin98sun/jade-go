package perfstat

import(
	"sync"
	"time"
	"math"
	"uta.edu/aces/jade-go/scheduler"
	ds "uta.edu/aces/jadesdk/data_structure"
)

type TaskCategoriesCache struct {
	mutex *sync.Mutex 
	clock uint64

	MaximumTaskAmount int
	DaemonIntervalInMilliseconds int
	HistoryTimeWindowSize int

	TaskCategories map[string]*TaskCategoryItem

	TaskHead *TaskPerfVector
	TaskTail  *TaskPerfVector
	TaskCount int
	TaskSLOExceedingCount int

	chanAverageSLORatios []chan AverageTaskSLORatios
}

func NewTaskCategoriesCache() *TaskCategoriesCache {

	c := &TaskCategoriesCache{
		mutex: &sync.Mutex{},
		TaskCategories: make(map[string]*TaskCategoryItem),
		chanAverageSLORatios: []chan AverageTaskSLORatios {},
	}

	go c.daemon()

	return c
}

func (p *TaskCategoriesCache) increaseClock() uint64 {
	if p.clock == math.MaxUint64 {
		p.clock = 0
	} else {
		p.clock++
	}
	return p.clock
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


func (p *TaskCategoriesCache) SubscribeAverageSLORatios(r chan AverageTaskSLORatios) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.chanAverageSLORatios = append(p.chanAverageSLORatios, r)
}

func (p *TaskCategoriesCache) SetHistoryTimeWindowSize(winodwSize int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.HistoryTimeWindowSize = winodwSize

}

func (p *TaskCategoriesCache) UnsafeRemoveTailTask() {
	expiredTask := p.TaskTail
	p.TaskTail = p.TaskTail.Before
	if p.TaskTail != nil {
		p.TaskTail.After = nil
	}
	p.TaskCount--
	expiredTask.TaskCategoryItem.RemoveTask(expiredTask)
}

func (p *TaskCategoriesCache) EnqueueArrivalTime(dispatchItem *ds.TaskDispatchingItem, arrivalTime time.Time, arrivalClock uint64, instantOverallArrivalRate float64, instantCumulativePerfVector *PerfEventVector) {

	taskTag := dispatchItem.GetUnifiedTag()

	p.mutex.Lock()
	defer p.mutex.Unlock()


	if _, e := p.TaskCategories[taskTag]; !e {
		percentile := dispatchItem.GetPercentile()
		slo := dispatchItem.GetTailLatencySLOInMilliseconds()
		p.TaskCategories[taskTag] = NewTaskCategoryItem(percentile, slo)
	}
	categoryItem := p.TaskCategories[taskTag]
	
	taskVector := categoryItem.ReserveForResponse(arrivalClock, dispatchItem, arrivalTime, instantOverallArrivalRate, instantCumulativePerfVector)
	taskVector.ArrivalTaskClock = p.clock

	if p.TaskHead != nil {
		taskVector.After = p.TaskHead
		p.TaskHead.Before = taskVector
	}
	p.TaskHead = taskVector
	p.TaskCount++

	if p.TaskTail == nil {
		p.TaskTail = taskVector
	} else if p.MaximumTaskAmount > 0 && p.TaskCount > p.MaximumTaskAmount {
		for p.TaskCount > p.MaximumTaskAmount {
			p.UnsafeRemoveTailTask()
		}
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

	perfVector := categoryItem.EnqueueResponse(dispatchItem, taskResponseTime, unloaded_tail_latency, queueing_budget, provision_overhead, aggregation_overhead, adjusted_unloaded_tail_latency, subtasks, instantOverallArrivalRate, instantCumulativePerfVector)
	perfVector.ResponseTaskClock = p.clock

	p.mutex.Unlock()

	return perfVector

}



