package perfstat

import(
	"time"
	"math"
)

func (p *TaskCategoriesCache) daemon() {
	for {
		p.mutex.Lock()
		time.Sleep(time.Duration(p.DaemonIntervalInMilliseconds) * time.Millisecond)


		for p.HistoryTimeWindowSize > 0 &&  p.TaskTail != nil &&
			(p.TaskTail.ArrivalTaskClock < p.clock - uint64(p.HistoryTimeWindowSize) || 
				( p.clock < uint64(p.HistoryTimeWindowSize) && 
				  p.TaskTail.ArrivalTaskClock > p.clock && 
				  p.TaskTail.ArrivalTaskClock < math.MaxUint64 - uint64(p.HistoryTimeWindowSize) - p.clock )) {
			p.UnsafeRemoveTailTask()
		}

		// calculate the average slo violation ratio

		if p.TaskCount > 0 && len(p.chanAverageSLORatios) > 0 {
			rv := float64(0)
			rs := float64(0)
			tn := float64(p.TaskCount)

			emptyCategories := []string{}
			for categoryKey, taskCategoryItem := range p.TaskCategories {
				if taskCategoryItem.TaskCount > 0 {
					tc := float64(taskCategoryItem.TaskCount)
					tv := float64(taskCategoryItem.SLOExceedingCount)

					pct := taskCategoryItem.PercentilePoint
					if pct > 1 {
						pct = pct / 100
					}

					w := tc / tn
					rv += math.Max(0, tv/tc - pct) * w
					rs += math.Max(0, pct - tv/tc) * w
				} else {
					emptyCategories = append(emptyCategories, categoryKey)
				}
			}

			for _, key := range emptyCategories {
				delete(p.TaskCategories, key)
			}

			// notify the receivers
			for _, c := range p.chanAverageSLORatios {
				r := AverageTaskSLORatios{
					Violation: rv,
					Surplus: rs,
				}
				c <- r
			}

		}

		p.increaseClock()
		p.mutex.Unlock()
	}
}