package histogram

import (
	"math"
	// "sync"
	// "time"
	// "uta.edu/aces/jade-go/kernel"
	"strconv"
	// "fmt"
	// "log"
)

type Histogram struct {
	Queue 		[]*HistogramItem
	RootItem	*HistogramItem
	QueueSize  	int64
	Count  		int64
	BucketHistogram *BucketHistogram
	Accuracy    float64
	MinItem 	*HistogramItem
	MaxItem     *HistogramItem
	Percentiles	map[string]*PercentileItem
}

type PercentileItem struct {
	Percentile  float64
	Item 		*HistogramItem
	Key 		string
	Count       int64
	RealPercentage float64
}


func PercentileKey(p float64) string{
	return strconv.FormatFloat(p, 'E', -1, 64)
}

func NewPercentileItem(p float64) *PercentileItem {
	return &PercentileItem{
		Percentile: p,
		Key: PercentileKey(p),
	}
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

func (h *Histogram) AddPercentilePoint(p float64) {
	item := NewPercentileItem(p)
	if h.Percentiles == nil {
		h.Percentiles = make(map[string]*PercentileItem)
	}
	h.Percentiles[item.Key] = item
}

func (h *Histogram) GetPercentile(p float64) *PercentileItem {
	if h.Percentiles == nil {return nil}
	key:=PercentileKey(p)
	if percentileItem , ok := h.Percentiles[key]; ok {
		return percentileItem
	} 
	return nil
}

// No matter how the histogram structure is implemented
// the most important three interfaces decide the overall performance

// the complexity of Enqueue shall be no larger than O(log n)
func (h *Histogram) Enqueue(incomingValue float64) *HistogramItem{

	v := h.UnifiedValue(incomingValue)

	var result *HistogramItem = nil

	var item *HistogramItem = nil
	var newRoot *HistogramItem = nil
	if h.RootItem != nil {
		item, newRoot = h.RootItem.Insert(v)
		if newRoot != nil {
			h.RootItem = newRoot
		}
		if h.MinItem.Smaller != nil {
			h.MinItem = h.MinItem.Smaller
		}
		if h.MaxItem.Larger != nil {
			h.MaxItem = h.MaxItem.Larger
		}

		total_count := float64(h.RootItem.Count)
		for _, p := range h.Percentiles {
			
			// before_v:=p.Item.Value

			if v <= p.Item.Value {
				p.Count += 1
				percentValue := float64(p.Count)/total_count
				p.RealPercentage = percentValue
				for x:=p.Item.Smaller; x != nil && percentValue > p.Percentile; x=x.Smaller {
					p.Item = x
					p.Count -= x.Larger.Duplications
					p.RealPercentage = float64(p.Count)/total_count
					percentValue = float64(p.Count-x.Duplications)/total_count
				}
			} else {
				percentValue := float64(p.Count)/total_count
				p.RealPercentage = percentValue
				for x:=p.Item.Larger; x != nil && percentValue < p.Percentile; x=x.Larger {
					percentValue = float64(p.Count + x.Duplications)/total_count
					if percentValue <= p.Percentile {
						p.Item = x
						p.Count += x.Duplications
						p.RealPercentage = float64(p.Count)/total_count
					}
				}
			}

			// after_v:=p.Item.Value
			// smaller_v:=float64(-1)
			// if p.Item.Smaller !=nil {
			// 	smaller_v = p.Item.Smaller.Value
			// }
			// larger_v:=float64(-1)
			// if p.Item.Larger !=nil {
			// 	larger_v = p.Item.Larger.Value
			// }
			// if before_v != after_v {
			// 	cc := p.Item.CumulativeCount()
			// 	log.Printf("inserted item %v, before %v, after: %v, smaller: %v, larger: %v,    percentile: %v, real: %v(%v/%v)[%v]", 
			// 		v, before_v, after_v, smaller_v, larger_v,
			// 		p.Percentile, float64(cc)/float64(h.RootItem.Count), 
			// 		cc, total_count, p.RealPercentage,
			// 	)	
			// }
		}
		
	} else {
		item = NewHistogramItem(v)
		h.RootItem = item
		h.MinItem = item
		h.MaxItem = item
		for _, p := range h.Percentiles {
			p.Item = item
			p.Count = 1
			p.RealPercentage = float64(1)
		}
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
		smaller := item.Smaller
		larger := item.Larger
		replacedItem, newRoot := item.Delete()
		is_node_removed := false
		if newRoot != nil || (newRoot == nil && replacedItem == nil) {
			is_node_removed = true
			h.RootItem = newRoot
			// the item is deleted 
			if item == h.MaxItem {
				h.MaxItem = smaller
			}
			if item == h.MinItem {
				h.MinItem = larger
			}
			h.BucketHistogram.Delete(item)
		}
		total_count := float64(h.RootItem.Count)
		deletedValue := item.Value
		for _, p := range h.Percentiles {
			// before_v:=p.Item.Value

			if item == p.Item || deletedValue <= p.Item.Value {
				p.Count--
				if item == p.Item || deletedValue == p.Item.Value {
					if is_node_removed {
						// item node is removed from the tree
						if larger != nil {
							p.Item = larger
							p.Count += larger.Duplications	
						} else if smaller != nil {
							p.Item = smaller
						} else {
							p.Item = nil
						}
					} 
				}
				percentile := float64(p.Count)/total_count
				p.RealPercentage = percentile

				if p.Item != nil {
					for x:=p.Item.Larger; x!=nil && percentile < p.Percentile; x=x.Larger {
						percentile = float64(p.Count + x.Duplications)/total_count
						if percentile <= p.Percentile {
							p.Item = x
							p.Count += x.Duplications
							p.RealPercentage = percentile
						}
					}
				}
			} else if deletedValue > p.Item.Value {
				percentile := float64(p.Count)/total_count
				p.RealPercentage = percentile
				for x:=p.Item.Smaller; x!=nil && percentile > p.Percentile; x=x.Smaller {
					p.Item = x
					p.Count -= x.Larger.Duplications
					p.RealPercentage = percentile
					percentile = float64(p.Count-x.Duplications)/total_count
				}
			}

			// after_v:=p.Item.Value
			// smaller_v:=float64(-1)
			// if p.Item.Smaller !=nil {
			// 	smaller_v = p.Item.Smaller.Value
			// }
			// larger_v:=float64(-1)
			// if p.Item.Larger !=nil {
			// 	larger_v = p.Item.Larger.Value
			// }
			// if before_v != after_v {
			// 	cc := p.Item.CumulativeCount()
			// 	log.Printf("deleted item %v, before %v, after: %v, smaller: %v, larger: %v,    percentile: %v, real: %v(%v/%v)[%v]", 
			// 		deletedValue, before_v, after_v, smaller_v, larger_v,
			// 		p.Percentile, float64(cc)/float64(h.RootItem.Count), 
			// 		cc, total_count, p.RealPercentage,
			// 	)	
			// }
			
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