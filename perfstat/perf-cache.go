package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	"sync"
	"time"
)


// PerfCache
type PerfCache struct {
	LengthPerCategory int64
	TaskCategories map[string]*TaskCategoryItem
	mutex *sync.Mutex 
}


func (p *PerfCache) Lock() {
	p.mutex.Lock()
}

func (p *PerfCache) Unlock() {
	p.mutex.Unlock()
}

func NewPerfCache() *PerfCache {
	return &PerfCache{
		TaskCategories: make(map[string]*TaskCategoryItem),
		mutex: &sync.Mutex{},
	}
}

func (p *PerfCache) EnqueueArrivalTime(dispatchItem *scheduler.TaskDispatchingItem, arrivalTime time.Time) {
	p.Lock()
	defer p.Unlock()

	taskTag := dispatchItem.GetUnifiedTag()

	if _, e := p.TaskCategories[taskTag]; !e {
		percentile := dispatchItem.GetPercentile()
		slo := dispatchItem.GetTailLatencySLOInMilliseconds()
		p.TaskCategories[taskTag] = NewTaskCategoryItem(percentile, slo)
	}

	categoryItem := p.TaskCategories[taskTag]
	categoryItem.EnqueueArrivalTime(arrivalTime)

}


func (p *PerfCache) EnqueueResponse(dispatchItem *scheduler.TaskDispatchingItem, finishTimestamp time.Time, subtasks map[string][]*scheduler.TaskCacheSubtaskItem) {

	p.Lock()
	defer p.Unlock()

	taskTag := dispatchItem.GetUnifiedTag()

	if _, e := p.TaskCategories[taskTag]; !e {
		p.TaskCategories[taskTag] = NewTaskCategoryItem()
	}

	categoryItem := p.TaskCategories[taskTag]
	taskResponseTime := float64(finishTimestamp.Sub(dispatchItem.ArriveTimestamp)/time.Millisecond)

	categoryItem.EnqueueResponse(taskResponseTime, dispatchItem, subtasks)

}
