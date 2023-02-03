package control_loop

import (
	"uta.edu/aces/jade-go/perfstat"
	"sync"
	"math"
)


type ControlLoop struct {
	Parameters *ControlLoopParameters
	mutex *sync.Mutex

	chanAverageSLORatios chan perfstat.AverageTaskSLORatios
	lastActionClock uint64
	isInAction bool
}


type ControlLoopParameters struct {
	IterationTimeScaleInMilliseconds int `json:"iterationTimeScaleInMilliseconds,omitempty"`
	MaximumTaskAmount int `json:"maximumTaskAmount,omitempty"`
	HistoryTimeWindowSize int `json:"historyTimeWindowSize,omitempty"`
	CalmDownTimeWindowSize int `json:"calmDownTimeWindowSize,omitempty"`
	ThresholdAverageSLOViolationRatio float64 `json:"thresholdAverageSLOViolationRatio,omitempty"`
	ThresholdAverageSLOSurplusRatio float64 `json:"thresholdAverageSLOSurplusRatio,omitempty"`
	ThresholdQueueingDeadlineViolationRatio float64 `json:"thresholdQueueingDeadlineViolationRatio,omitempty"`
	ThresholdQueueingDeadlineSurplusRatio float64 `json:"thresholdQueueingDeadlineSurplusRatio,omitempty"`
}

func NewControlLoop(c chan perfstat.AverageTaskSLORatios) *ControlLoop {
	loop := &ControlLoop{
		mutex: &sync.Mutex{},
		Parameters: &ControlLoopParameters{},
		chanAverageSLORatios: c,
	}
	go loop.daemon()
	return loop
}

func (l *ControlLoop) isSetup() bool {
	if l.Parameters != nil {
		if l.Parameters.HistoryTimeWindowSize > 0 {
			if l.Parameters.CalmDownTimeWindowSize > 0 {
				if l.Parameters.ThresholdAverageSLOViolationRatio > 0 {
					if l.Parameters.ThresholdAverageSLOSurplusRatio > 0 {
						if l.Parameters.ThresholdQueueingDeadlineViolationRatio > 0 {
							if l.Parameters.ThresholdQueueingDeadlineSurplusRatio > 0 {
								return true
							}
						}
					}
				}
			}
		}
	}
	return false
}

func (l *ControlLoop) isOutofCalmdownWindow (clock uint64) bool {

	if clock > l.lastActionClock + uint64(l.Parameters.CalmDownTimeWindowSize) {
		return true
	} else if l.lastActionClock + uint64(l.Parameters.CalmDownTimeWindowSize) - math.MaxUint64 > 0 {
		if clock < l.lastActionClock {
			if clock > l.lastActionClock + uint64(l.Parameters.CalmDownTimeWindowSize) - math.MaxUint64 {
				return true
			}
		}
	}

	return false
}

func (l *ControlLoop) daemon() {
	for {
		taskSLORatios :=  <-l.chanAverageSLORatios
		
		l.mutex.Lock()
		if l.isSetup() && !l.isInAction && l.isOutofCalmdownWindow(taskSLORatios.Clock) {

			if taskSLORatios.Violation >= l.Parameters.ThresholdAverageSLOViolationRatio {
				l.isInAction = true
				l.lastActionClock = taskSLORatios.Clock
				go l.ScaleUp(taskSLORatios.Violation)
			} else if taskSLORatios.Surplus <= l.Parameters.ThresholdAverageSLOSurplusRatio {
				l.isInAction = true
				l.lastActionClock = taskSLORatios.Clock
				go l.ScaleDown(taskSLORatios.Surplus)
			}

		}
		l.mutex.Unlock()
	}
}

func (l *ControlLoop) actionIsDone() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.isInAction = false
}

func (l *ControlLoop) ScaleUp(currentViolationRatio float64) {



	l.actionIsDone()
}


func (l *ControlLoop) ScaleDown(currentSurplusRatio float64) {



	l.actionIsDone()
}


