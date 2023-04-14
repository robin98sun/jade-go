package perfstat


import (
	"uta.edu/aces/jade-go/scheduler"
	"time"
	"uta.edu/aces/jadesdk"
	ds "uta.edu/aces/jadesdk/data_structure"
)


type AppPerfCache struct {

	TaskCategories *TaskCategoriesCache

	ArrivalRateTracker *ArrivalRateTracker
	
	PerfEventMatrices *PerfEventMatrixPipe

	Clock *Clock

	AppKey string
	
}


func NewAppPerfCache(appKey string, clock *Clock, EVENTMatrixSize int, EVENTPipeLength int) *AppPerfCache {
	return &AppPerfCache{
		TaskCategories: NewTaskCategoriesCache(CloneClock(clock)),
		ArrivalRateTracker: NewArrivalRateTracker(10),
		PerfEventMatrices: NewPerfEventMatrixPipe(CloneClock(clock), EVENTPipeLength, EVENTMatrixSize, 0.99),
		Clock: clock,
		AppKey: appKey,
	}
}

func (p *AppPerfCache) Clear() {
	p.TaskCategories.Clear()
	p.ArrivalRateTracker = NewArrivalRateTracker(10)
	p.PerfEventMatrices = NewPerfEventMatrixPipe(p.Clock, EVENTPipeLength, EVENTMatrixSize, 0.99)
}

func (p *AppPerfCache) SubscribeAverageSLORatios(m MessengerAverageTaskSLORatios) {
	p.TaskCategories.SubscribeAverageSLORatios(m)
}

func (p *AppPerfCache) SetMaximumTaskAmount(maximumTaskAmount int) {
	p.TaskCategories.SetMaximumTaskAmount(maximumTaskAmount)
}

func (p *AppPerfCache) SetIterationTimeScaleInMilliseconds(timeScale int) {
	p.TaskCategories.SetIterationTimeScaleInMilliseconds(timeScale)
	p.PerfEventMatrices.SetIterationTimeScaleInMilliseconds(timeScale)
	p.Clock.SetIterationTimeScaleInMilliseconds(timeScale)
}

func (p *AppPerfCache) SetHistoryTimeWindowSize(winodwSize int) {
	p.TaskCategories.SetHistoryTimeWindowSize(winodwSize)
	p.PerfEventMatrices.SetHistoryTimeWindowSize(winodwSize)
}


func (p *AppPerfCache) AppendQueueDeadlineViolationEvent(queueKey string, deadlineViolationTime float64, budget float64) {
	p.PerfEventMatrices.AppendQueueDeadlineViolationEvent(queueKey, deadlineViolationTime, budget)
}

func (p *AppPerfCache) AppendQueueServiceResponseTimeEvent(queueKey string, serviceResponseTime float64) {
	p.PerfEventMatrices.AppendQueueServiceResponseTimeEvent(queueKey, serviceResponseTime)
}

func (p *AppPerfCache) EnqueueEnvMetrics(queueKey string, metrics *jadesdk.MetricsEnv) {
	p.PerfEventMatrices.AppendEnvPerfEvent(queueKey, metrics)
}

func (p *AppPerfCache) EnqueueArrivalTime(dispatchItem *ds.TaskDispatchingItem, arrivalTime time.Time) {
	
	arrivalClock := p.Clock.CurrentClock()

	_, instantOverallArrivalRate := p.ArrivalRateTracker.Enqueue(arrivalTime)

	instantCumulativePerfVector := p.PerfEventMatrices.GetInstantCumulativePerfVector()

	p.TaskCategories.EnqueueArrivalTime(dispatchItem, arrivalTime, arrivalClock, instantOverallArrivalRate, instantCumulativePerfVector)
}


func (p *AppPerfCache) EnqueueResponse(dispatchItem *ds.TaskDispatchingItem, unloaded_tail_latency float64, queueing_budget float64, provision_overhead float64, aggregation_overhead float64, adjusted_unloaded_tail_latency float64, finishTimestamp time.Time, subtasks map[string][]*scheduler.TaskCacheSubtaskItem) {

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
			perfVector.ResponseEventClock = responseClock
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


func (p *AppPerfCache) CollectTaskTraces(traceType string, printf func(string, ...interface{})) [][]string {

	queueSet := p.PerfEventMatrices.GetQueueClocks()
	return p.TaskCategories.CollectTraces(traceType, queueSet, printf)

}

func (p *AppPerfCache) GetQueuesAsPerDeadlineViolation(appKey string, deadlineViolationRatio float64) []*QueuePerfMessage {
	return p.PerfEventMatrices.GetQueuesAsPerDeadlineViolation(appKey, deadlineViolationRatio)
}

func (p *AppPerfCache) GetQueuesAsPerDeadlineSurplus(appKey string, deadlineSurplusRatio float64) []*QueuePerfMessage {
	return p.PerfEventMatrices.GetQueuesAsPerDeadlineSurplus(appKey, deadlineSurplusRatio)
}
