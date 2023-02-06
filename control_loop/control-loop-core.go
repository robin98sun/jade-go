package control_loop

import (
	"uta.edu/aces/jade-go/perfstat"
	"sync"
	"math"
)

type ActionStatus struct {
	StartClock uint64
	TimeoutClock  uint64
	CompleteClock uint64
}
func NewActionStatus(clock uint64) *ActionStatus {
	return &ActionStatus{
		StartClock: clock,
	}
}
func (a *ActionStatus) Start(clock uint64) {
	a.StartClock = clock
	a.TimeoutClock = 0
	a.CompleteClock = 0
}
func (a *ActionStatus) IsStarted() bool {
	if a.StartClock > 0 {return true}
	return false
}
func (a *ActionStatus) IsComplete() bool {
	if a.CompleteClock > 0 || a.TimeoutClock > 0 {
		return true
	}
	return false
}
func (a *ActionStatus) IsInAction() bool {
	return a.IsStarted() && !a.IsComplete()
}

func (a *ActionStatus) GetStopClock() uint64 {
	if a.TimeoutClock > a.CompleteClock {return a.TimeoutClock}
	return a.CompleteClock
}

type ControlLoop struct {
	Parameters *ControlLoopParameters
	mutex *sync.Mutex

	chanAverageSLORatios chan perfstat.AverageTaskSLORatios

	actionStatusPerApp map[string]*ActionStatus

	MessengerQueuesAsPerDeadlineViolation *func(appKey string, deadlineViolationThreshold float64) []*perfstat.QueuePerfMessage
	MessengerQueuesAsPerDeadlineSurplus *func(appKey string, deadlineSurplusThreshold float64) []*perfstat.QueuePerfMessage

	PerfMessageBuffer []*perfstat.PerfMessage

	DaemonIntervalInMilliseconds int

	clock *perfstat.Clock
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
	ActionTimeOutWindowSize int `json:"actionTimeOut,omitempty"`
}

func NewControlLoop(clock *perfstat.Clock) *ControlLoop {
	loop := &ControlLoop{
		mutex: &sync.Mutex{},
		Parameters: &ControlLoopParameters{},
		actionStatusPerApp: map[string]*ActionStatus{},
		PerfMessageBuffer: []*perfstat.PerfMessage{},
		DaemonIntervalInMilliseconds: 100,
		clock: clock,
	}
	go loop.daemon()
	return loop
}

func (l *ControlLoop) SetIterationTimeScaleInMilliseconds(timeScale int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.DaemonIntervalInMilliseconds = timeScale
}

func (l *ControlLoop) AppendPerfMessage(msg *perfstat.PerfMessage) {
	if msg == nil {return}

	l.mutex.Lock()
	l.mutex.Unlock()

	l.PerfMessageBuffer = append(l.PerfMessageBuffer, msg)
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

func (l *ControlLoop) isOutofCalmdownWindow (appKey string, clock uint64) bool {

	if _, e := l.actionStatusPerApp[appKey]; !e {
		return true
	}

	if l.actionStatusPerApp[appKey].IsInAction() {return false}
	if !l.actionStatusPerApp[appKey].IsStarted() {return false}

	stopClock := l.actionStatusPerApp[appKey].GetStopClock()

	if clock > stopClock + uint64(l.Parameters.CalmDownTimeWindowSize) {
		return true
	} else if stopClock + uint64(l.Parameters.CalmDownTimeWindowSize) - math.MaxUint64 > 0 {
		if clock < stopClock {
			if clock > stopClock + uint64(l.Parameters.CalmDownTimeWindowSize) - math.MaxUint64 {
				return true
			}
		}
	}

	return false
}


func (l *ControlLoop) getScaleUpCandidates(appKey string, threshold float64) []*perfstat.QueuePerfMessage {

	if l.MessengerQueuesAsPerDeadlineViolation == nil {return nil}

	return (*l.MessengerQueuesAsPerDeadlineViolation)(appKey, threshold)

}


func (l *ControlLoop) getScaleDownCandidates(appKey string, threshold float64) []*perfstat.QueuePerfMessage {

	if l.MessengerQueuesAsPerDeadlineSurplus == nil {return nil}

	return (*l.MessengerQueuesAsPerDeadlineSurplus)(appKey, threshold)

}


