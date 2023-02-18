package resource_manager

import (
	"uta.edu/aces/jade-go/perfstat"
	"sync"
	"math"
	"strconv"
	"sort"
	ds "uta.edu/aces/jadesdk/data_structure"
)

type ScalingActionType string
const(
	ScalingActionTypeUp ScalingActionType = "up"
	ScalingActionTypeDown ScalingActionType = "down"
)

const DefaultLocalResourceManagerPort int = 8765

type ScalingAction struct {
	ActionKey string
	AppKey string
	QueueKey string
	PodUIDs []string
	ActionType ScalingActionType
	Ratio float64
	AverageRatio float64
	StartClock uint64
	CompleteClock uint64
	Succeeded bool
	SourceNode *ds.Node
	ActionGroupID string
}

type ScalingResult struct {
	ActionKey string
	Succeeded bool
}

func (a *ScalingAction) GetKey() string {
	if a.ActionKey != "" {return a.ActionKey}

	a.ActionKey = a.AppKey + "--" + a.QueueKey + "--" + strconv.FormatUint(a.StartClock, 16)

	return a.ActionKey
}

type ActionStatus struct {
	StartClock uint64
	TimeoutClock  uint64
	CompleteClock uint64
	Succeeded bool
	ActionList map[string][]*ScalingAction
}
func NewActionStatus(clock uint64) *ActionStatus {
	return &ActionStatus{
		StartClock: clock,
		ActionList: make(map[string][]*ScalingAction),
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

	MessengerScaleQueue *func(appKey string, queueKey string, action *ScalingAction) bool

	MessengerReportScalingResult *func(node *ds.Node, result *ScalingResult) bool

	MessengerPodUIDsAsPerQueue *func(appKey string, queueKey string) []string

	MessengerCommLocalResourceManagerAddon *func(port int, method string, path string, payload interface{}, response interface{}) error

	PerfMessageBuffer []*perfstat.PerfMessage

	DaemonIntervalInMilliseconds int

	clock *perfstat.Clock

	ActionCache map[string]*ScalingAction

	CPUResourceCache *CPUResourceCache

	DefaultUnitForVerticalScaling float64
	DefaultUnitForHorizontalScaling float64

	LocalResourceManagerPort int

	EnableAutoScaling bool

}

func (l *ControlLoop) SwitchAutoScaling(on bool) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.EnableAutoScaling = on
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
	LocalResourceManagerPort int `json:"localResourceManagerPort,omitempty"`
}

func DefaultControlLoopParameters() *ControlLoopParameters {
	return &ControlLoopParameters{
		IterationTimeScaleInMilliseconds: 100,
		MaximumTaskAmount: 0,
		HistoryTimeWindowSize: 600,
		CalmDownTimeWindowSize: 150,
		ThresholdAverageSLOViolationRatio: 0.01,
		ThresholdAverageSLOSurplusRatio: 0.1,
		ThresholdQueueingDeadlineViolationRatio: 0.01,
		ThresholdQueueingDeadlineSurplusRatio: 0.1,
		ActionTimeOutWindowSize: 150,
		LocalResourceManagerPort: 8765,
	}
}

func NewControlLoop(clock *perfstat.Clock) *ControlLoop {
	loop := &ControlLoop{
		mutex: &sync.Mutex{},
		Parameters: DefaultControlLoopParameters(),
		actionStatusPerApp: map[string]*ActionStatus{},
		PerfMessageBuffer: []*perfstat.PerfMessage{},
		DaemonIntervalInMilliseconds: 100,
		clock: clock,
		ActionCache: make(map[string]*ScalingAction),
		CPUResourceCache: NewCPUResourceCache(),
		DefaultUnitForVerticalScaling: 0.1,
		DefaultUnitForHorizontalScaling: 1,
		LocalResourceManagerPort: DefaultLocalResourceManagerPort,
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


func (l *ControlLoop) SendAction(appKey string, queueKey string, action *ScalingAction) {
	l.mutex.Lock()
	if l.MessengerScaleQueue == nil {
		l.mutex.Unlock()
		return
	}

	actionKey := action.GetKey()
	l.ActionCache[actionKey] = action

	l.mutex.Unlock()

	success := (*l.MessengerScaleQueue)(appKey, queueKey, action)

	if !success {
		l.mutex.Lock()
		currentClock := l.clock.CurrentClock()
		l.actionStatusPerApp[appKey].CompleteClock = currentClock
		l.actionStatusPerApp[appKey].Succeeded = false
		delete(l.ActionCache, actionKey)
		// l.ActionCache[actionKey].CompleteClock = currentClock
		// l.ActionCache[actionKey].Succeeded = false
		l.mutex.Unlock()
	}
}



func (l *ControlLoop) ActionHasBeenPhysicallyExecuted(result *ScalingResult) {
	if result == nil || result.ActionKey == "" {return}

	l.mutex.Lock()
	if action, e := l.ActionCache[result.ActionKey]; e {
		currentClock := l.clock.CurrentClock()
		l.actionStatusPerApp[action.AppKey].CompleteClock = currentClock
		l.actionStatusPerApp[action.AppKey].Succeeded = result.Succeeded
		action.CompleteClock = currentClock
		delete(l.ActionCache, result.ActionKey)
	}
	l.mutex.Unlock()

}

func (l *ControlLoop) generateActionGroupID(appKey string, startClock uint64) string {
	return appKey + "::" + strconv.FormatUint(startClock, 10)
}

func (l *ControlLoop) GetActionHistoryTrace() [][]string {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	result := [][]string{}
	for appKey, appHistory := range l.actionStatusPerApp {

		clock_list := []uint64{}
		for _, actionGroup := range appHistory.ActionList {
			if len(actionGroup) == 0 {continue}
			actionGroupClock := actionGroup[0].StartClock
			clock_list = append(clock_list, actionGroupClock)
		}
		sort.Slice(clock_list, func(i, j int) bool {
			return clock_list[i] < clock_list[j]
		})

		for _, clock := range clock_list {
			groupId := l.generateActionGroupID(appKey, clock)
			if actionGroup, e := appHistory.ActionList[groupId]; e {
				if len(actionGroup) == 0 {continue}
				line := []string{
					appKey, strconv.FormatUint(clock, 10),
					string(actionGroup[0].ActionType),
				}
				result = append(result, line)
			}
		}

	}
	return result
}


