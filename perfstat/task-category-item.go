package perfstat

import (
	"uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	// "sync"
	"time"
)


type TaskCategoryItem struct {
	HistogramPipeOfTaskResponseTime []*histogram.Histogram
	MatrixPipeOfSubtaskPerf []*SubtaskPerfMatrix
	ArrivalRateTrackers []*ArrivalRateTracker
	HistCount   int
	HistLength  int
	SliceLength int
	SliceCount  int
	PercentilePoint float64
	TailLatencySLO float64
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
	}

	for i:=0; i<histCount; i++ {
		hist := histogram.NewHistogram(int64(histLength), float64(0.1), 1)
		hist.AddPercentilePoint(percentile)
		tci.HistogramPipeOfTaskResponseTime = append(tci.HistogramPipeOfTaskResponseTime, hist)
	}

	
	return tci
}

func (t *TaskCategoryItem) EnqueueArrivalTime(arrivalTime time.Time) {

	dequeuedTime := arrivalTime
	for i:=0; i<len(t.ArrivalRateTrackers);i++ {
		if time.Time.IsZero(dequeuedTime) {
			break
		}
		tracker := t.ArrivalRateTrackers[i]
		dequeuedTime = tracker.Enqueue(arrivalTime)
	}
	if !time.Time.IsZero(dequeuedTime) && len(t.ArrivalRateTrackers) < t.SliceCount {
		newTracker := NewArrivalRateTracker(t.SliceLength)
		newTracker.Enqueue(dequeuedTime)
		t.ArrivalRateTrackers = append(t.ArrivalRateTrackers, newTracker)
	}
}

func (t *TaskCategoryItem) EnqueueResponse(taskResponseTime float64, dispatchItem *scheduler.TaskDispatchingItem, subtasks map[string][]*scheduler.TaskCacheSubtaskItem) {

	// subtask performance matrix
	vector := NewSubtaskPerfVector(dispatchItem, subtasks)

	dequeuedVector := vector
	for i:= 0; i<len(t.MatrixPipeOfSubtaskPerf); i++ {
		matrix := t.MatrixPipeOfSubtaskPerf[i]
		dequeuedVector = matrix.Enqueue(dequeuedVector)
		if dequeuedVector == nil {
			break
		}
	}

	if dequeuedVector != nil && len(t.MatrixPipeOfSubtaskPerf) < t.SliceCount {
		newMatrix := NewSubtaskPerfMatrix(t.HistLength)
		t.MatrixPipeOfSubtaskPerf = append(t.MatrixPipeOfSubtaskPerf, newMatrix)
		newMatrix.Enqueue(dequeuedVector)
	}

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

}

