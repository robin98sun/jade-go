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
	MatrixPipeOfSubtaskPerf []*TaskPerfMatrix
	ArrivalRateTracker *ArrivalRateTracker
	HistCount   int
	HistLength  int
	SliceLength int
	SliceCount  int
	PercentilePoint float64
	TailLatencySLO float64
	TaskCount int
	SLOExceedingCount int
	mutex   *sync.Mutex
}


func NewTaskCategoryItem(percentile float64, slo float64) *TaskCategoryItem {
	
	histLength := 10000
	histCount := 1
	sliceLength := 10
	// sliceCount := 100000
	sliceCount := 0

	tci := &TaskCategoryItem{
		HistCount: histCount,
		HistLength: histLength,
		SliceLength: sliceLength,
		SliceCount:  sliceCount,
		PercentilePoint: percentile,
		TailLatencySLO: slo,
		HistogramPipeOfTaskResponseTime: []*histogram.Histogram{},
		MatrixPipeOfSubtaskPerf: []*TaskPerfMatrix{},
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

func (t *TaskCategoryItem) ReserveForResponse(currentClock uint64, dispatchItem *ds.TaskDispatchingItem, arrivalTime time.Time, instantOverallArrivalRate float64, cumulativePerfVector *PerfEventVector) *TaskPerfVector {
	vector := NewTaskPerfVector(dispatchItem)
	vector.ArrivalEventClock = currentClock

	dequeuedVector := vector

	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.TaskCount++

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

	if dequeuedVector != nil && (t.SliceCount <= 0 || len(t.MatrixPipeOfSubtaskPerf) < t.SliceCount) {
		newMatrix := NewTaskPerfMatrix(t.SliceLength)
		t.MatrixPipeOfSubtaskPerf = append(t.MatrixPipeOfSubtaskPerf, newMatrix)
		newMatrix.Enqueue(dequeuedVector)
	} else if dequeuedVector != nil {
		t.TaskCount--
		if dequeuedVector.TaskResponseTime > t.TailLatencySLO {
			t.SLOExceedingCount--
		}
	}

	vector.MostRecentCumulativePerfVectorAtBeginning = cumulativePerfVector

	if dispatchItem != nil && dispatchItem.Options!=nil && dispatchItem.Options.DispatchingRatePerSecond > 0 {
		vector.DispatchingRate = dispatchItem.Options.DispatchingRatePerSecond 
	}

	vector.TaskCategoryItem = t

	return vector
}

func (t *TaskCategoryItem) EnqueueResponse(dispatchItem *ds.TaskDispatchingItem,taskResponseTime float64, unloaded_tail_latency float64, queueing_budget float64,provision_overhead float64, aggregation_overhead float64, adjusted_unloaded_tail_latency float64,subtasks map[string][]*scheduler.TaskCacheSubtaskItem, instantOverallArrivalRate float64, cumulativePerfVector *PerfEventVector) *TaskPerfVector {

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
			_, vector := matrix.GetVector(taskKey)
			vector.IncarnateSubtasks(subtasks)
			vector.TaskResponseTime = taskResponseTime
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

func (t *TaskCategoryItem) RemoveTask(task *TaskPerfVector) bool{

	if task == nil {return false}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	taskKey := task.GetTaskKey()

	matrixIdx := -1
	for i:= 0; i<len(t.MatrixPipeOfSubtaskPerf); i++ {
		matrix := t.MatrixPipeOfSubtaskPerf[i]
		idx := matrix.RemoveVector(taskKey)
		if idx >= 0 {
			t.TaskCount--
			if task.TaskResponseTime > t.TailLatencySLO {
				t.SLOExceedingCount--
			}
			matrixIdx = i
		}
	}

	if matrixIdx >= 0 {
		matrix := t.MatrixPipeOfSubtaskPerf[matrixIdx]
		if matrix.GetVectorCount() == 0 {
			tmpList := []*TaskPerfMatrix{}
			for i:=0; i<matrixIdx; i++ {
				tmpList = append(tmpList, t.MatrixPipeOfSubtaskPerf[i])
			}
			for i:=matrixIdx+1; i<len(t.MatrixPipeOfSubtaskPerf); i++ {
				tmpList = append(tmpList, t.MatrixPipeOfSubtaskPerf[i])
			}
			t.MatrixPipeOfSubtaskPerf = tmpList
		}
		return true
	}
	return false
}

