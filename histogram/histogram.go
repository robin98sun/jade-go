package histogram

import (
	"math"
	// "sync"
	// "time"
	// "uta.edu/aces/jade-go/kernel"
	"strconv"
	"fmt"
	"log"
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

func (h *Histogram) GetIndexOfSubHistogram(v float64) int {
	idx, _, _ := h.BucketHistogram.CalcPosition(v)
	return int(idx)
}

func (h *Histogram) GetLengthOfSubHistograms() int {
	return len(h.BucketHistogram.SubBucketHistograms)
}


func (h *Histogram) GetMaximumSizeOfSubHistograms() int {
	return int(math.Round(h.BucketHistogram.SubBucketHistogramSize*h.Accuracy))
}

func (h *Histogram) GetValueOfBucket(subhistogramIndex int, bucketIndex int) float64 {
	lower_boundary, _ := h.BucketHistogram.GetLowerAndUpperBoundaries(int64(subhistogramIndex))
	return lower_boundary + float64(bucketIndex) * h.Accuracy
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

func SearchPercentileByMultiply(
		p float64, start_value float64, 
		histogram_list []*Histogram, 
		opt_out_mask []bool, 
		lower_subhistogram_index int, 
		upper_subhistogram_index int, 
		is_going_up bool, 
		subhistogram_index int,
		last_prod float64,
		last_criteria float64,
		iteration_count int,
		DEBUG bool,
	) (float64){
	
	if lower_subhistogram_index > upper_subhistogram_index {
		return float64(-1)
	}

	mid := (lower_subhistogram_index+upper_subhistogram_index)/2
	lower_boundary, upper_boundary := float64(0), float64(0)
	criteria_value := start_value
	if start_value < 0 {
		if subhistogram_index < 0 {
			lower_boundary, upper_boundary = histogram_list[0].BucketHistogram.GetLowerAndUpperBoundaries(int64(mid))
			criteria_value = lower_boundary
			if is_going_up {
				criteria_value = upper_boundary
			}
		} else {
			criteria_value = histogram_list[0].GetValueOfBucket(subhistogram_index, mid)
		}
		
	} 

	var burnt_out_indices []int

	multiply_histograms := func(criteria float64) (float64) {
		burnt_out_indices = []int{}
		product := float64(1)
		for i:=0; i<len(histogram_list); i++ {
			if len(opt_out_mask) > i && opt_out_mask[i] {continue}

			h := histogram_list[i]
			node := h.RootItem.FindNoLargerThan(criteria)
			if node == h.MaxItem {
				burnt_out_indices = append(burnt_out_indices, i)
			} else {
				cc := node.CumulativeCount()
				product *= float64(cc)/float64(h.Count)
			}
		}
		return product
	}

	prod := multiply_histograms(criteria_value)

	lower, upper := lower_subhistogram_index, upper_subhistogram_index
	go_up := true

	got_the_result := false

	if prod == p {
		got_the_result = true
	} else {
		is_going_to_try_the_other_boundary := []string{"first time", "retry"}
		if subhistogram_index >= 0 {
			is_going_to_try_the_other_boundary = []string{"first time"}
		}
		sizeOfSubhistogram := histogram_list[0].GetMaximumSizeOfSubHistograms()
		need_retry := false
		for _, x := range  is_going_to_try_the_other_boundary{
			if x == "retry" && !need_retry {
				break
			} else {
			}
			if prod < p {
				for _, i := range burnt_out_indices {
					if len(opt_out_mask) > i {
						opt_out_mask[i] = true
					}
				}

				if subhistogram_index < 0 && start_value < 0{
					if x == "first time" {
						if !is_going_up {
							prod = multiply_histograms(upper_boundary)
							need_retry = true
							continue
						}
					} else if x == "retry" && is_going_up {
						// fall into this subhistogram range
						return SearchPercentileByMultiply(
							p, -1, histogram_list, opt_out_mask,
							0, sizeOfSubhistogram-1, true, 
							mid, prod, criteria_value,
							1, DEBUG,
						)
					}
				}
				
				lower = mid+1
				
			} else if prod > p {
				burnt_out_indices = nil

				if subhistogram_index < 0 && start_value < 0 {
					if x == "first time" {
						if is_going_up {
							prod = multiply_histograms(lower_boundary)
							need_retry = true
							continue
						}
					} else if x == "retry" && !is_going_up {
						// fall into this subhistogram range
						return SearchPercentileByMultiply(
							p, -1, histogram_list, opt_out_mask,
							0, sizeOfSubhistogram-1, true, 
							mid, prod, criteria_value,
							1, DEBUG,
						)
						break
					}
				}

				upper = mid-1
				go_up = false

				
			}
		}
	}

	if DEBUG {
		if subhistogram_index < 0 && iteration_count == 1 {
			log.Printf("iterations to search %v percentile:", p*float64(100))
		}
		placeholder := "" 
		if subhistogram_index < 0 {
			placeholder = " "
		} else {
			placeholder = fmt.Sprintf("   in [%v] subhistogram, ", subhistogram_index)
		}
		directionStr := ""
		if got_the_result || lower > upper {
			directionStr = ", stop"
		} else if go_up {
			directionStr = ", next go up"
		} else {
			directionStr = ", next go down"
		}
		log.Printf("%v%v iteration: %v, burn out %v histograms, idx: %v, lower: %v, upper: %v, criteria: %v%v",
			placeholder, iteration_count,
			prod, len(burnt_out_indices),
			mid, lower_subhistogram_index, upper_subhistogram_index,
			math.Round(criteria_value*10)/10, directionStr,
		)
	}
	

	if got_the_result {
		return criteria_value
	} else if lower > upper {
		if last_criteria >= 0 && last_prod >= 0 {
			if math.Abs(p-last_prod) < math.Abs(p-prod) {
				if DEBUG {
					log.Print("   due to larger distance, the last iteration is discarded")
				}
				return last_criteria
			}
		}
		return criteria_value
	} else {
		return SearchPercentileByMultiply(
			p, -1, histogram_list, opt_out_mask,
			lower, upper, go_up, 
			subhistogram_index, 
			prod, criteria_value,
			iteration_count+1,
			DEBUG,
		)
	}
}