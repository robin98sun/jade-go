package control_loop

import (
	// // "uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/perfstat"
	"sync"
	// "time"
	// "strconv"
	// "sort"
	// "uta.edu/aces/jadesdk"
	// ds "uta.edu/aces/jadesdk/data_structure"
)


type ControlLoop struct {
	Parameters *ControlLoopParameters
	mutex *sync.Mutex

	chanAverageSLOViolationRatio chan float64
}


type ControlLoopParameters struct {
	IterationTimeScaleInMilliseconds int `json:"iterationTimeScaleInMilliseconds,omitempty"`
	MaximumTaskAmount int `json:"maximumTaskAmount,omitempty"`
	HistoryTimeWindowSize int `json:"historyTimeWindowSize,omitempty"`
	CalmDownTimeWindowSize int `json:"calmDownTimeWindowSize,omitempty"`
	AverageSLOViolationRatio float64 `json:"averageSLOViolationRatio,omitempty"`
	QueueingDeadlineViolationRatio float64 `json:"queueingDeadlineViolationRatio,omitempty"`
	QueueingDeadlineSurplusRatio float64 `json:"queueingDeadlineSurplusRatio,omitempty"`
}

func NewControlLoop(c chan perfstat.AverageTaskSLORatios) *ControlLoop {
	return &ControlLoop{
		mutex: &sync.Mutex{},
		Parameters: &ControlLoopParameters{},
	}
}

func (c *ControlLoop) Lock() {
	c.mutex.Lock()
}

func (c *ControlLoop) Unlock() {
	c.mutex.Unlock()
}

