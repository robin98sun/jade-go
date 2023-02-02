package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	"time"
	"uta.edu/aces/jadesdk"
	ds "uta.edu/aces/jadesdk/data_structure"
)

const(
	EVENTMatrixSize int = 600
	EVENTPipeLength int = 0
)

// PerfCache
type PerfCache struct {

	TaskCategories *TaskCategoriesCache

	ArrivalRateTracker *ArrivalRateTracker
	
	PerfEventMatrices *PerfEventMatrixPipe
}


func NewPerfCache() *PerfCache {
	return &PerfCache{
		TaskCategories: NewTaskCategoriesCache(),
		ArrivalRateTracker: NewArrivalRateTracker(10),
		PerfEventMatrices: NewPerfEventMatrixPipe(EVENTPipeLength, EVENTMatrixSize, 0.99),
	}
}

func (p *PerfCache) Clear() {

	p.TaskCategories.Clear()
	p.ArrivalRateTracker = NewArrivalRateTracker(10)
	p.PerfEventMatrices = NewPerfEventMatrixPipe(EVENTPipeLength, EVENTMatrixSize, 0.99)
}

func (p *PerfCache) SubscribeAverageSLOViolationRatio(c chan float64) {
	p.PerfEventMatrices.SubscribeAverageSLOViolationRatio(c)
}

func (p *PerfCache) SetMaximumTaskAmount(maximumTaskAmount int) {
	p.TaskCategories.SetMaximumTaskAmount(maximumTaskAmount)
}

func (p *PerfCache) SetIterationTimeScaleInMilliseconds(timeScale int) {
	p.TaskCategories.SetIterationTimeScaleInMilliseconds(timeScale)
	p.PerfEventMatrices.SetIterationTimeScaleInMilliseconds(timeScale)
}

func (p *PerfCache) AppendQueueDeadlineViolationEvent(queueKey string, deadlineViolationTime float64) {
	p.PerfEventMatrices.AppendQueueDeadlineViolationEvent(queueKey, deadlineViolationTime)
}

func (p *PerfCache) AppendQueueServiceResponseTimeEvent(queueKey string, serviceResponseTime float64) {
	p.PerfEventMatrices.AppendQueueServiceResponseTimeEvent(queueKey, serviceResponseTime)
}

func (p *PerfCache) EnqueueEnvMetrics(queueKey string, metrics *jadesdk.MetricsEnv) {
	p.PerfEventMatrices.AppendEnvPerfEvent(queueKey, metrics)
}

func (p *PerfCache) EnqueueArrivalTime(dispatchItem *ds.TaskDispatchingItem, arrivalTime time.Time) {
	

	arrivalClock := p.PerfEventMatrices.GetEventClock()

	_, instantOverallArrivalRate := p.ArrivalRateTracker.Enqueue(arrivalTime)

	instantCumulativePerfVector := p.PerfEventMatrices.GetInstantCumulativePerfVector()

	p.TaskCategories.EnqueueArrivalTime(dispatchItem, arrivalTime, arrivalClock, instantOverallArrivalRate, instantCumulativePerfVector)
}


func (p *PerfCache) EnqueueResponse(dispatchItem *ds.TaskDispatchingItem, unloaded_tail_latency float64, queueing_budget float64, provision_overhead float64, aggregation_overhead float64, adjusted_unloaded_tail_latency float64, finishTimestamp time.Time, subtasks map[string][]*scheduler.TaskCacheSubtaskItem) {

	instantOverallArrivalRate := p.ArrivalRateTracker.GetArrivalRatePerSecond()
	instantCumulativePerfVector := p.PerfEventMatrices.GetInstantCumulativePerfVector()
	taskResponseTime := float64(finishTimestamp.Sub(dispatchItem.ArriveTimestamp)/time.Millisecond)

	perfVector := p.TaskCategories.EnqueueResponse(
						dispatchItem, unloaded_tail_latency, queueing_budget, 
						provision_overhead, aggregation_overhead, adjusted_unloaded_tail_latency, 
						taskResponseTime, subtasks, 
						instantOverallArrivalRate, instantCumulativePerfVector,
					)

	if perfVector != nil {
		callback := func(responseClock uint64) {
			perfVector.ResponseClock = responseClock
		}
		p.PerfEventMatrices.AppendTaskPerfEvent(
			dispatchItem.GetTailLatencySLOInMilliseconds(),
			dispatchItem.GetPercentile(),
			taskResponseTime,
			perfVector.GetEventsOfStrugglingQueues(dispatchItem.GetTailLatencySLOInMilliseconds(), provision_overhead, aggregation_overhead),
			&callback,
		)
	}

}


func (p *PerfCache) CollectTraces(traceType string, printf func(string, ...interface{})) [][]string {

	queueSet := p.PerfEventMatrices.GetQueueClocks()
	return p.TaskCategories.CollectTraces(traceType, queueSet, printf)

}
