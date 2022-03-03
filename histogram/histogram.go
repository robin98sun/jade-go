package histogram

import (
	"math"
	// "sync"
	// "time"
	// "uta.edu/aces/jade-go/kernel"
	// "strconv"
	// "fmt"
)

type Histogram struct {
	Queue 		[]*HistogramItem
	RootItem	*HistogramItem
	QueueSize  	int64
	Count  		int64
	BucketHistogram *BucketHistogram
	Accuracy    float64
}

func NewHistogram(size int64, subBucketHistogramSize float64, accuracy int) *Histogram {
	bs := float64(1)
	accuracy_factor := math.Pow(10, float64(accuracy))
	if accuracy != 0 {
		bs = float64(1) / accuracy_factor
	} 

	sbs := subBucketHistogramSize
	if subBucketHistogramSize == 0 {
		sbs = float64(10.0)
	}

	h := &Histogram{
		Queue: []*HistogramItem{},
		QueueSize: size,
		BucketHistogram: NewBucketHistogram(sbs, bs),
		Accuracy: accuracy_factor,
	}
	return h
}

func (h *Histogram) UnifiedValue(value float64) float64 {
	v := value

	v = math.Round(v* h.Accuracy)/h.Accuracy
	return v
}

// No matter how the histogram structure is implemented
// the most important three interfaces decide the overall performance

// the complexity of Enqueue shall be no larger than O(log n)
func (h *Histogram) Enqueue(value float64) *HistogramItem{

	v := h.UnifiedValue(value)

	var result *HistogramItem = nil

	var item *HistogramItem = nil
	var newRoot *HistogramItem = nil
	if h.RootItem != nil {
		item, newRoot = h.RootItem.Insert(v)
		if newRoot != nil {
			h.RootItem = newRoot
		}
	} else {
		item = NewHistogramItem(v)
		h.RootItem = item
	}
	if item != nil && item.Duplications == 1{
		h.BucketHistogram.Insert(item)
	}
	h.Queue = append(h.Queue, item)
	h.Count += 1
	if h.QueueSize > 0 && h.Count > h.QueueSize {
		result = h.Dequeue()
	}
	return result
}

// the complexity of Dequeue shall be no larger than O(log n)
func (h *Histogram) Dequeue() *HistogramItem {
	var item *HistogramItem = nil

	if len(h.Queue) > 0 {
		item = h.Queue[0]
		h.Queue = h.Queue[1:]
		h.Count -= 1
		replacedItem, newRoot := item.Delete()
		if newRoot != nil || (newRoot == nil && replacedItem == nil) {
			h.RootItem = newRoot
			h.BucketHistogram.Delete(item)
		}
	}
	return item
}

// the complexity of GetByPercentage shall be as close as O(1)
func (h *Histogram) GetByPercentage(p float64) float64 {
	result := -1.0

	return result
}

// the complexity of CountPercentageByValue shall be as close as O(1)
func (h *Histogram) CountPercentageByValue(v float64, startPercentage float64) float64 {
	result := -1.0

	return result
}