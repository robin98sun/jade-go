package perfstat

import (
	"time"
	"sync"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jadesdk"
	ds "uta.edu/aces/jadesdk/data_structure"
)

type AverageTaskSLORatios struct {
	Clock     uint64
	AppKey    string
	Violation float64
	Surplus   float64
}

type PerfMessageType string
const(
	PerfMessageTypeAvgTaskSLORatios PerfMessageType = "avg_task_slo_ratios"
)

type PerfMessage struct {
	Type PerfMessageType
	AvgTaskSLORatios *AverageTaskSLORatios
}

type MessengerAverageTaskSLORatios func(m *PerfMessage)

const(
	EVENTMatrixSize int = 100
	EVENTPipeLength int = 0
)

// PerfCache
type PerfCache struct {

	AppPerfSlots map[string]*AppPerfCache

	mutex *sync.Mutex
	Clock *Clock

	// chanAverageSLORatios []chan AverageTaskSLORatios
	MaximumTaskAmount int
	IterationTimeScaleInMilliseconds int
	HistoryTimeWindowSize int

	listMessengerAverageTaskSLORatios []MessengerAverageTaskSLORatios
}


func NewPerfCache() *PerfCache {
	clock := NewClock()
	return &PerfCache{
		// TaskCategories: NewTaskCategoriesCache(clock),
		// ArrivalRateTracker: NewArrivalRateTracker(10),
		// PerfEventMatrices: NewPerfEventMatrixPipe(clock, EVENTPipeLength, EVENTMatrixSize, 0.99),

		AppPerfSlots: make(map[string]*AppPerfCache),

		mutex: &sync.Mutex{},
		Clock: clock,

		// chanAverageSLORatios: []chan AverageTaskSLORatios{},
		listMessengerAverageTaskSLORatios: []MessengerAverageTaskSLORatios{},
	}
}

func (p *PerfCache) Clear() {

	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, app := range p.AppPerfSlots {
		app.Clear()
	}
}

func (p *PerfCache) SubscribeAverageSLORatios(m MessengerAverageTaskSLORatios) {

	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.listMessengerAverageTaskSLORatios = append(p.listMessengerAverageTaskSLORatios, m)

	for _, app := range p.AppPerfSlots {
		app.TaskCategories.SubscribeAverageSLORatios(m)
	}
}

// Maximum Task Amount
func (p *PerfCache) SetMaximumTaskAmount(maximumTaskAmount int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.MaximumTaskAmount = maximumTaskAmount
	for _, app := range p.AppPerfSlots {
		p.setMaximumTaskAmountForAppSlot(app)
	}
}

func (p *PerfCache) setMaximumTaskAmountForAppSlot(app *AppPerfCache) {
	app.TaskCategories.SetMaximumTaskAmount(p.MaximumTaskAmount)
}

// Iteration TimeScale In Milliseconds
func (p *PerfCache) SetIterationTimeScaleInMilliseconds(timeScale int) {

	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.IterationTimeScaleInMilliseconds = timeScale
	p.Clock.SetIterationTimeScaleInMilliseconds(timeScale)

	for _, app := range p.AppPerfSlots {
		p.setIterationTimeScaleInMillisecondsForAppSlot(app)
	}

}

func (p *PerfCache) setIterationTimeScaleInMillisecondsForAppSlot(app *AppPerfCache) {

	app.TaskCategories.SetIterationTimeScaleInMilliseconds(p.IterationTimeScaleInMilliseconds)
	app.PerfEventMatrices.SetIterationTimeScaleInMilliseconds(p.IterationTimeScaleInMilliseconds)

}

// history time window size
func (p *PerfCache) SetHistoryTimeWindowSize(winodwSize int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.HistoryTimeWindowSize = winodwSize

	for _, app := range p.AppPerfSlots {
		p.setHistoryTimeWindowSizeForAppSlot(app)
	}

}

func (p *PerfCache) setHistoryTimeWindowSizeForAppSlot(app *AppPerfCache) {
	app.TaskCategories.SetHistoryTimeWindowSize(p.HistoryTimeWindowSize)
	app.PerfEventMatrices.SetHistoryTimeWindowSize(p.HistoryTimeWindowSize)
}

// new app slot
func (p *PerfCache) getOrNewAppSlot(appKey string) *AppPerfCache {
	if existingApp, e := p.AppPerfSlots[appKey]; e {
		return existingApp
	} 

	app := NewAppPerfCache(appKey, p.Clock, EVENTMatrixSize, EVENTPipeLength)
	p.setMaximumTaskAmountForAppSlot(app)
	p.setHistoryTimeWindowSizeForAppSlot(app)
	p.setIterationTimeScaleInMillisecondsForAppSlot(app)
	for _, c := range p.listMessengerAverageTaskSLORatios {
		app.TaskCategories.SubscribeAverageSLORatios(c)
	}

	p.AppPerfSlots[appKey] = app

	return app
}

// Perf Cache API
func (p *PerfCache) AppendQueueDeadlineViolationEvent(appKey string, queueKey string, deadlineViolationTime float64, budget float64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	app := p.getOrNewAppSlot(appKey)
	app.PerfEventMatrices.AppendQueueDeadlineViolationEvent(queueKey, deadlineViolationTime, budget)
}

func (p *PerfCache) AppendQueueServiceResponseTimeEvent(appKey string, queueKey string, serviceResponseTime float64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	app := p.getOrNewAppSlot(appKey)
	app.PerfEventMatrices.AppendQueueServiceResponseTimeEvent(queueKey, serviceResponseTime)
}

func (p *PerfCache) EnqueueEnvMetrics(appKey string, queueKey string, metrics *jadesdk.MetricsEnv) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	app := p.getOrNewAppSlot(appKey)
	app.PerfEventMatrices.AppendEnvPerfEvent(queueKey, metrics)
}

func (p *PerfCache) EnqueueArrivalTime(appKey string, dispatchItem *ds.TaskDispatchingItem, arrivalTime time.Time) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	app := p.getOrNewAppSlot(appKey)
	
	arrivalClock := p.Clock.CurrentClock()

	_, instantOverallArrivalRate := app.ArrivalRateTracker.Enqueue(arrivalTime)

	instantCumulativePerfVector := app.PerfEventMatrices.GetInstantCumulativePerfVector()

	app.TaskCategories.EnqueueArrivalTime(dispatchItem, arrivalTime, arrivalClock, instantOverallArrivalRate, instantCumulativePerfVector)
}


func (p *PerfCache) EnqueueResponse(appKey string, dispatchItem *ds.TaskDispatchingItem, unloaded_tail_latency float64, queueing_budget float64, provision_overhead float64, aggregation_overhead float64, adjusted_unloaded_tail_latency float64, finishTimestamp time.Time, subtasks map[string][]*scheduler.TaskCacheSubtaskItem) {

	p.mutex.Lock()
	defer p.mutex.Unlock()

	app := p.getOrNewAppSlot(appKey)

	instantOverallArrivalRate := app.ArrivalRateTracker.GetArrivalRatePerSecond()
	instantCumulativePerfVector := app.PerfEventMatrices.GetInstantCumulativePerfVector()
	taskResponseTime := float64(finishTimestamp.Sub(dispatchItem.ArriveTimestamp)/time.Millisecond)

	perfVector := app.TaskCategories.EnqueueResponse(
						dispatchItem, unloaded_tail_latency, queueing_budget, 
						provision_overhead, aggregation_overhead, adjusted_unloaded_tail_latency, 
						taskResponseTime, subtasks, 
						instantOverallArrivalRate, instantCumulativePerfVector,
					)

	if perfVector != nil {
		callback := func(responseClock uint64) {
			perfVector.ResponseEventClock = responseClock
		}
		app.PerfEventMatrices.AppendTaskPerfEvent(
			dispatchItem.GetTailLatencySLOInMilliseconds(),
			dispatchItem.GetPercentile(),
			taskResponseTime,
			perfVector.GetEventsOfStrugglingQueues(dispatchItem.GetTailLatencySLOInMilliseconds(), provision_overhead, aggregation_overhead),
			&callback,
		)
	}

}

// func (p *PerfCache) Collecto


func (p *PerfCache) CollectTaskTraces(traceType string, printf func(string, ...interface{})) [][]string {

	p.mutex.Lock()
	defer p.mutex.Unlock()


	result := [][]string{}
	for _, app := range p.AppPerfSlots{

		queueSet := app.PerfEventMatrices.GetQueueClocks()
		subResult := app.TaskCategories.CollectTraces(traceType, queueSet, printf)

		result = append(result, subResult...)

	}

	return result

}

func (p *PerfCache) CollectEventTraces(traceType string, printf func(string, ...interface{})) [][]string {

	p.mutex.Lock()
	defer p.mutex.Unlock()


	result := [][]string{}
	for _, app := range p.AppPerfSlots{

		subResult := app.PerfEventMatrices.CollectTraces(printf)

		result = append(result, subResult...)

	}

	return result

}


func (p *PerfCache) StartPerfEventListener() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, app := range p.AppPerfSlots {
		app.PerfEventMatrices.StartListener()
	}
}

func (p *PerfCache) StopPerfEventListener() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, app := range p.AppPerfSlots {
		app.PerfEventMatrices.StopListener()
	}
}