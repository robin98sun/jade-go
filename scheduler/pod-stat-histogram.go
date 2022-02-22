package scheduler

import (
	// "math"
	// "sync"
	// "time"
	// "uta.edu/aces/jade-go/kernel"
)

type Histogram struct {
	Queue 		[]*HistogramItem
	RootItem	*HistogramItem
	QueueSize  	int64
	Count       int64
}

// No matter how the histogram structure is implemented
// the most important three interfaces decide the overall performance

// the complexity of Enqueue shall be no larger than O(log n)
func (h *Histogram) Enqueue(v float64) *HistogramItem{
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
	h.Queue = append(h.Queue, item)
	h.Count += 1
	if h.QueueSize > 0 && h.Count > h.QueueSize {
		result = h.Dequeue()
	}
	return result
}

// the complexity of Dequeue shall be no larger than O(log n)
func (h *Histogram) Dequeue() *HistogramItem {
	var result *HistogramItem = nil

	if len(h.Queue) > 0 {
		result = h.Queue[0]
		h.Queue = h.Queue[1:]
		h.Count -= 1
		replacedItem, newRoot := result.Delete()
		if newRoot != nil || (newRoot == nil && replacedItem == nil) {
			h.RootItem = newRoot
		}
	}
	return result
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