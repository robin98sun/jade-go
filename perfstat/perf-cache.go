package perfstat

import (
	"uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	"sync"
	"time"
)

type SubtaskPerfItem struct {
	ResponseTime      		float64 // without queueing, equal with "unloaded service response time"
									// but including communication time
	GivenBudget             float64 // the budget (in milliseconds) has been assigned to the subtask
	DeadlineViolationTime 	float64
	CommunicationTime		float64
	QueueingTime            float64
}


type SubtaskPerfVector struct {
	DispatchItem *scheduler.TaskDispatchingItem
	SubtaskPerf  map[string]*SubtaskPerfItem
}

type SubtaskPerfMatrix struct {
	VectorPipeOfSubtaskPerf []*SubtaskPerfVector
}

func (m *SubtaskPerfMatrix) Enqueue(dispatchItem *scheduler.TaskDispatchingItem, subtasks []*scheduler.SubtaskOnNode) {
	vector := &SubtaskPerfVector{
		DispatchItem: dispatchItem,
		SubtaskPerf: make(map[string]*SubtaskPerfItem),
	}

	for _, snItem := range subtasks {

	}

}


type TaskCategoryItem struct {
	HistogramPipeOfTaskResponseTime []*histogram.Histogram
	MatrixPipeOfSubtaskPerf []*SubtaskPerfMatrix
}


func NewTaskCategoryItem() *TaskCategoryItem {
	pipeLength := 100
	histLength := 10000

	tci := &TaskCategoryItem{
		HistogramPipeOfTaskResponseTime: []*histogram.Histogram{},
	}

	for i:=0; i<pipeLength; i++ {
		hist := histogram.NewHistogram(int64(histLength), float64(0.1), 1)
		hist.AddPercentilePoint(float64(0.99))
		hist.AddPercentilePoint(float64(0.995)) // (0.99)^1/2
		hist.AddPercentilePoint(float64(0.997)) // (0.99)^1/3
		hist.AddPercentilePoint(float64(0.9975)) // (0.99)^1/4
		// hist.AddPercentilePoint(float64(0.998)) // (0.99)^1/5
		// hist.AddPercentilePoint(float64(0.9983)) // (0.99)^1/6
		// hist.AddPercentilePoint(float64(0.9986)) // (0.99)^1/7
		// hist.AddPercentilePoint(float64(0.9987)) // (0.99)^1/8
		// hist.AddPercentilePoint(float64(0.9987)) // (0.99)^1/8
		// hist.AddPercentilePoint(float64(0.9989)) // (0.99)^1/9
		// hist.AddPercentilePoint(float64(0.999)) // (0.99)^1/10

		tci.HistogramPipeOfTaskResponseTime = append(tci.HistogramPipeOfTaskResponseTime, hist)
	}
	
	return tci
}

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

func (p *PerfCache) Enqueue(dispatchItem *scheduler.TaskDispatchingItem, finishTimestamp time.Time, subtasks []*scheduler.SubtaskOnNode) {

	p.Lock()
	defer p.Unlock()

	taskTag := dispatchItem.GetUnifiedTag()

	if _, e := p.TaskCategories[taskTag]; !e {
		p.TaskCategories[taskTag] = NewTaskCategoryItem()
	}

	categoryItem := p.TaskCategories[taskTag]
	taskResponsTime := float64(finishTimestamp.Sub(dispatchItem.ArriveTimestamp)*10/time.Millisecond)/float64(10)


}
