package perfstat

import (
	"uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	"sync"
	"time"
	"unsafe"
)


type TaskCategoryItem struct {
	HistogramPipeOfTaskResponseTime []*histogram.Histogram
	MatrixPipeOfSubtaskPerf []*SubtaskPerfMatrix
	ArrivalRateTrackers []*ArrivalRateTracker
	OverallArrivalRates [][]float64
	HistCount   int
	HistLength  int
	SliceLength int
	SliceCount  int
	PercentilePoint float64
	TailLatencySLO float64
	MemoryOccupation uintptr
	mutex   *sync.Mutex
}


func NewTaskCategoryItem(percentile float64, slo float64) *TaskCategoryItem {
	
	histLength := 1000
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
		ArrivalRateTrackers: []*ArrivalRateTracker{},
		OverallArrivalRates: [][]float64{},
		MemoryOccupation: 0,
		mutex: &sync.Mutex{},
	}

	for i:=0; i<histCount; i++ {
		hist := histogram.NewHistogram(int64(histLength), float64(0.1), 1)
		hist.AddPercentilePoint(percentile)
		tci.HistogramPipeOfTaskResponseTime = append(tci.HistogramPipeOfTaskResponseTime, hist)
	}

	tci.MemoryOccupation = unsafe.Sizeof(tci)

	
	return tci
}

func (t *TaskCategoryItem) EnqueueOverallArrivalRate(arrivalRate float64) float64 {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	
	dequeuedArrivalRate := arrivalRate

	for i := 0; i<len(t.OverallArrivalRates); i++ {
		if dequeuedArrivalRate < 0 {
			break
		}
		t.OverallArrivalRates[i] = append(t.OverallArrivalRates[i], dequeuedArrivalRate)
		if len(t.OverallArrivalRates[i]) > t.SliceLength {
			dequeuedArrivalRate = t.OverallArrivalRates[i][0]
			t.OverallArrivalRates[i] = t.OverallArrivalRates[i][1:]
		} else {
			dequeuedArrivalRate = -1
		}
	}

	if dequeuedArrivalRate >= 0 && len(t.OverallArrivalRates) < t.SliceCount {
		t.OverallArrivalRates = append(t.OverallArrivalRates, []float64{dequeuedArrivalRate})
		dequeuedArrivalRate = -1
	}

	return dequeuedArrivalRate

}

func (t *TaskCategoryItem) EnqueueArrivalTime(arrivalTime time.Time) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	dequeuedTime := arrivalTime
	for i:=0; i<len(t.ArrivalRateTrackers);i++ {
		if time.Time.IsZero(dequeuedTime) {
			break
		}
		tracker := t.ArrivalRateTrackers[i]
		dequeuedTime, _ = tracker.Enqueue(arrivalTime)
	}
	if !time.Time.IsZero(dequeuedTime) && len(t.ArrivalRateTrackers) < t.SliceCount {
		newTracker := NewArrivalRateTracker(t.SliceLength)
		newTracker.Enqueue(dequeuedTime)
		t.ArrivalRateTrackers = append(t.ArrivalRateTrackers, newTracker)
	}
	t.MemoryOccupation = unsafe.Sizeof(t)
}

func (t *TaskCategoryItem) EnqueueResponse(taskResponseTime float64, unloaded_tail_latency float64, adjusted_unloaded_tail_latency float64, dispatchItem *scheduler.TaskDispatchingItem, subtasks map[string][]*scheduler.TaskCacheSubtaskItem) {

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

	
	vector := NewSubtaskPerfVector(dispatchItem, subtasks, tail, unloaded_tail_latency, adjusted_unloaded_tail_latency)

	dequeuedVector := vector
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

	t.MemoryOccupation = unsafe.Sizeof(t)
	vector.MemoryOccupation = t.MemoryOccupation

}

