package perfstat

import (
	"uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	"sync"
	"time"
	ds "uta.edu/aces/jadesdk/data_structure"
)


type TaskCategoryItem struct {
	HistogramPipeOfTaskResponseTime []*histogram.Histogram
	MatrixPipeOfSubtaskPerf []*SubtaskPerfMatrix
	ArrivalRateTracker *ArrivalRateTracker
	HistCount   int
	HistLength  int
	SliceLength int
	SliceCount  int
	PercentilePoint float64
	TailLatencySLO float64
	mutex   *sync.Mutex
}


func NewTaskCategoryItem(percentile float64, slo float64) *TaskCategoryItem {
	
	histLength := 10000
	histCount := 1
	sliceLength := 10
	sliceCount := 10000

	tci := &TaskCategoryItem{
		HistCount: histCount,
		HistLength: histLength,
		SliceLength: sliceLength,
		SliceCount:  sliceCount,
		PercentilePoint: percentile,
		TailLatencySLO: slo,
		HistogramPipeOfTaskResponseTime: []*histogram.Histogram{},
		MatrixPipeOfSubtaskPerf: []*SubtaskPerfMatrix{},
		ArrivalRateTracker: NewArrivalRateTracker(sliceLength),
		mutex: &sync.Mutex{},
	}

	for i:=0; i<histCount; i++ {
		hist := histogram.NewHistogram(int64(histLength), float64(0.1), 1)
		hist.AddPercentilePoint(percentile)
		tci.HistogramPipeOfTaskResponseTime = append(tci.HistogramPipeOfTaskResponseTime, hist)
	}
	
	return tci
}

func (t *TaskCategoryItem) ReserveForResponse(currentClock uint64, dispatchItem *ds.TaskDispatchingItem, arrivalTime time.Time, instantOverallArrivalRate float64, cumulativePerfVector *PerfEventVector) {
	vector := NewSubtaskPerfVector(dispatchItem)
	vector.ArrivalClock = currentClock

	dequeuedVector := vector

	t.mutex.Lock()
	defer t.mutex.Unlock()

	vector.InstantOverallArrivalRateAtBeginning = instantOverallArrivalRate
	_, vector.InstantTaskArrivalRateAtBeginning = t.ArrivalRateTracker.Enqueue(arrivalTime)

	// enqueue the vector
	for i:= 0; i<len(t.MatrixPipeOfSubtaskPerf); i++ {
		matrix := t.MatrixPipeOfSubtaskPerf[i]
		dequeuedVector = matrix.Enqueue(dequeuedVector)
		if dequeuedVector == nil {
			break
		}
	}

	if dequeuedVector != nil && len(t.MatrixPipeOfSubtaskPerf) < t.SliceCount {
		newMatrix := NewSubtaskPerfMatrix(t.SliceLength)
		t.MatrixPipeOfSubtaskPerf = append(t.MatrixPipeOfSubtaskPerf, newMatrix)
		newMatrix.Enqueue(dequeuedVector)
	}

	vector.MostRecentCumulativePerfVectorAtBeginning = cumulativePerfVector

	if dispatchItem != nil && dispatchItem.Options!=nil && dispatchItem.Options.DispatchingRatePerSecond > 0 {
		vector.DispatchingRate = dispatchItem.Options.DispatchingRatePerSecond 
	}
}

func (t *TaskCategoryItem) EnqueueResponse(dispatchItem *ds.TaskDispatchingItem,taskResponseTime float64, unloaded_tail_latency float64, queueing_budget float64,provision_overhead float64, aggregation_overhead float64, adjusted_unloaded_tail_latency float64,subtasks map[string][]*scheduler.TaskCacheSubtaskItem, instantOverallArrivalRate float64, cumulativePerfVector *PerfEventVector) *SubtaskPerfVector {

	t.mutex.Lock()
	defer t.mutex.Unlock()
	
	// histogram of task response time
	dequeuedValue := taskResponseTime
	for i:=0; i<len(t.HistogramPipeOfTaskResponseTime); i++ {
		if dequeuedValue < 0 {
			break
		}
		histogram := t.HistogramPipeOfTaskResponseTime[i]
		dequeuedHistogramItem := histogram.Enqueue(dequeuedValue, 1)
		if dequeuedHistogramItem != nil {
			dequeuedValue = dequeuedHistogramItem.Value
		} else {
			dequeuedValue = float64(-1)
		}
	}

	// subtask performance matrix
	var tail float64
	if len(t.HistogramPipeOfTaskResponseTime) > 0 {
		tail = t.HistogramPipeOfTaskResponseTime[0].GetValueAtPercentile(t.PercentilePoint)
	}

	taskKey := "N/A"
	if dispatchItem != nil && dispatchItem.Task != nil {
		taskKey = dispatchItem.Task.GetKey()
	}
	for i:= 0; i<len(t.MatrixPipeOfSubtaskPerf); i++ {
		matrix := t.MatrixPipeOfSubtaskPerf[i]
		if matrix.TaskExist(taskKey) {
			vector := matrix.GetVector(taskKey)
			vector.IncarnateSubtasks(subtasks)
			vector.TailLatency = tail
			vector.UnloadedTailLatency = unloaded_tail_latency
			vector.QueueingBudget = queueing_budget
			vector.ProvisionOverhead = provision_overhead
			vector.AggregationOverhead = aggregation_overhead
			vector.AdjustedUnloadedTaillatency = adjusted_unloaded_tail_latency
			vector.InstantOverallArrivalRateAtEnd = instantOverallArrivalRate
			vector.InstantTaskArrivalRateAtEnd = t.ArrivalRateTracker.GetArrivalRatePerSecond()
			vector.MostRecentCumulativePerfVectorAtEnd = cumulativePerfVector
			
			return vector
		}
	}

	return nil
}

