package perfstat

import(
	"time"
	"math"
)

func (p *TaskCategoriesCache) daemon() {	
	INTERVAL := 100
	if p.DaemonIntervalInMilliseconds > 0 {
		INTERVAL = p.DaemonIntervalInMilliseconds
	}
	for {
		time.Sleep(time.Duration(p.DaemonIntervalInMilliseconds) * time.Millisecond)

		p.mutex.Lock()

		if p.DaemonIntervalInMilliseconds > 0 && p.DaemonIntervalInMilliseconds != INTERVAL {
			INTERVAL = p.DaemonIntervalInMilliseconds
		}

		currentClock := p.clock.CurrentClock()

		for p.HistoryTimeWindowSize > 0 &&  p.TaskTail != nil &&
			(p.TaskTail.ArrivalTaskClock <  currentClock - uint64(p.HistoryTimeWindowSize) || 
				( currentClock < uint64(p.HistoryTimeWindowSize) && 
				  p.TaskTail.ArrivalTaskClock > currentClock && 
				  p.TaskTail.ArrivalTaskClock < math.MaxUint64 - uint64(p.HistoryTimeWindowSize) - currentClock )) {
			p.UnsafeRemoveTailTask()
		}

		// calculate the average slo violation and surplus ratios
		if p.TaskCount > 0 && len(p.chanAverageSLORatios) > 0 {
			rv := float64(0)
			rs := float64(0)
			tn := float64(p.TaskCount)

			for _, taskCategoryItem := range p.TaskCategories {
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
				}
			}

			// notify the receivers
			for _, c := range p.chanAverageSLORatios {
				r := AverageTaskSLORatios{
					Clock: currentClock,
					Violation: rv,
					Surplus: rs,
				}
				c <- r
			}

		}

		// remove empty categories
		if len(p.TaskCategories) > 0 {
			emptyCategories := []string{}
			for categoryKey, taskCategoryItem := range p.TaskCategories {
				if taskCategoryItem.TaskCount <= 0 {
					emptyCategories = append(emptyCategories, categoryKey)
				}
			}
			for _, key := range emptyCategories {
				delete(p.TaskCategories, key)
			}
		}

		p.mutex.Unlock()
	}
}