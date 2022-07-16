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
}


func NewTaskCategoryItem() *TaskCategoryItem {
	
	histLength := 1000
	histCount := 100
	sliceLength := 10
	sliceCount := histLength * histCount / sliceLength

	tci := &TaskCategoryItem{
		HistCount: histCount,
		HistLength: histLength,
		SliceLength: sliceLength,
		SliceCount:  sliceCount,
		HistogramPipeOfTaskResponseTime: []*histogram.Histogram{},
		MatrixPipeOfSubtaskPerf: []*SubtaskPerfMatrix{},
		ArrivalRateTrackers: []*ArrivalRateTracker{},
	}

	for i:=0; i<histCount; i++ {
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

