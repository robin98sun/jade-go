package perfstat

import (
	"uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	"sync"
	"time"
)


type TaskCategoryItem struct {
	HistogramTaskResponseTime *histogram.Histogram
}


func NewTaskCategoryItem() *TaskCategoryItem {
	h_tr := histogram.NewHistogram(1000, float64(0.1), 1)
	h_tr.AddPercentilePoint(float64(0.99))
	return &TaskCategoryItem{
		HistogramTaskResponseTime: h_tr,
	}
}


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

func (p *PerfCache) Enqueue(dispatchItem *scheduler.TaskDispatchingItem, finishTimestamp time.Time, subtasks []*scheduler.SubtaskOnNode) {

	p.Lock()
	defer p.Unlock()

	taskTag := dispatchItem.GetUnifiedTag()

	if _, e := p.TaskCategories[taskTag]; !e {
		p.TaskCategories[taskTag] = NewTaskCategoryItem()
	}

	categoryItem := p.TaskCategories[taskTag]
	taskResponsTime := float64(finishTimestamp.Sub(dispatchItem.ArriveTimestamp)*10/time.Millisecond)/float64(10)

	categoryItem.HistogramTaskResponseTime.Enqueue(taskResponsTime, 1)

}
