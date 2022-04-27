package scheduler

import (
	"time"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jadesdk"
	"fmt"
)


type BudgetNegotiationType string
const (
	BudgetNegotiationTypeHistogram BudgetNegotiationType = "histogram"
	BudgetNegotiationTypeChoicesOfMeanAndVariance = "choices_mean_and_variance"
	BudgetNegotiationTypeGivenMeanAndVariance = "given_mean_and_variance"
	BudgetNegotiationTypeGivenBudgetTime = "given_budget_time"
)

type TaskDispatchingOptions struct {
	ForceUpdateNetworkStructure bool                 `json:"forceUpdateNetworkStructure,omitempty"`
	SaveResultInCache         	bool   `json:"saveResultInCache,omitempty"`
	PersistCache              	bool   `json:"persistCache,omitempty"`
	EstimatedServiceTimeModel 	string `json:"estimatedServiceTimeModel,omitempty"` // "exponential"/"poission", "constant"
	EstimatedMeanServiceTime  	float64  `json:"estimatedMeanServiceTime,omitempty"`  // for "exponential" / "poission"
	ServiceTimeList        		[]float64 `json:"serviceTimeList,omitempty"` // in milliseconds
	SortSubnodes 				bool `json:"sortSubnodes,omitempty"` // whether sort the available subnodes
	BudgetNegotiation           BudgetNegotiationType `json:"budgetNegotiation,omitempty"`
	CDFPoints                   int 	`json:"cdfPoints,omitempty"`
	CDFStartPoint				float64 `json:"cdfStartPoint,omitempty"`
	BudgetEstimationPercentilePoint float64 `json:"budgetEstimationPercentilePoint,omitempty"`
}

const TaskDefaultPriority = 1000
type TaskDispatchingItem struct {
	Task            *kernel.Task                            `json:"task,omitempty"`
	ReportTo        map[string]*TaskDispatchingItemReportTo `json:"reportTo,omitempty"` // moduleName: reportTo
	SLO 			*TaskDispatchingItemSLO 				`json:"slo,omitempty"`
	Budgets         map[string]*TaskDispatchingItemBudget   `json:"budgets,omitempty"`  // moduleName: budget
	Options         *TaskDispatchingOptions                 `json:"options,omitempty"`
	Priority	    int                                     `json:"priority,omitempty"`

	// timestamps
	ArriveTimestamp time.Time
	InquiryStartTimestamp time.Time
	InquiryDoneTimestamp time.Time
	BudgetEstimationDoneTimestamp time.Time

	// TTL is the maximum broadcast domains it can out reach
	// if set 0, it means only within the autonomy service domain
	// if set 1, it means broadcast in current broadcast domain which contain multiple neighboring ASDs
	TTL             int64 	`json:"ttl,omitempty"` 
}

func (t *TaskDispatchingItem) copy(withReport bool, minimum bool) *TaskDispatchingItem {
	inst := &TaskDispatchingItem{}
	if t.Task != nil {
		inst.Task = t.Task
	}
	if minimum {
		return inst
	}

	if t.Budgets != nil {
		inst.Budgets = t.Budgets
	}
	if t.Options != nil {
		inst.Options = t.Options
	}
	inst.ArriveTimestamp = t.ArriveTimestamp
	if withReport {
		if t.ReportTo != nil && len(t.ReportTo) > 0 {
			inst.ReportTo = make(map[string]*TaskDispatchingItemReportTo)
			for k, v := range t.ReportTo {
				inst.ReportTo[k] = v.Copy()
			}
		}
	}
	if t.Priority != 0 {
		inst.Priority = t.Priority
	}
	if !t.ArriveTimestamp.IsZero() {
		inst.ArriveTimestamp = t.ArriveTimestamp
	}
	if t.SLO != nil {
		inst.SLO = t.SLO
	}
	inst.TTL = t.TTL

	return inst
}

func (t *TaskDispatchingItem) MinimumCopy() *TaskDispatchingItem {
	return t.copy(false, true)
}

func (t *TaskDispatchingItem) CopyForSubtask(withReport bool) *TaskDispatchingItem {
	inst := t.copy(withReport, false)
	if t.Task != nil {
		inst.Task = t.Task.CopyForSubtask()
	}
	return inst
}

func (t *TaskDispatchingItem) Arrived() {
	if t != nil {
		t.ArriveTimestamp = time.Now()
	}
}

func (t *TaskDispatchingItem) GetArriveTime() time.Time {
	return t.ArriveTimestamp
}


type TaskDispatchingItemReportTo struct {
	Node *jadesdk.Node `json:"node,omitempty"`
	Pod  *kernel.Pod  `json:"pod,omitempty"`
}

func (r *TaskDispatchingItemReportTo) Copy() *TaskDispatchingItemReportTo {
	if r == nil {
		return nil
	}
	inst := &TaskDispatchingItemReportTo{
		Node: r.Node,
		Pod:  r.Pod,
	}
	return inst
}

func (r *TaskDispatchingItemReportTo) Desc() string {
	if r == nil {
		return "nil"
	}
	desc := "[report to]:"
	if r.Node != nil {
		desc = fmt.Sprintf("%v [node addr: %v, port: %v]", desc, r.Node.Addr, r.Node.Port)
	}
	if r.Pod != nil {
		desc = fmt.Sprintf("%v [pod addr: %v, port: %v]", desc, r.Pod.Addr, r.Pod.Port)
	}
	return desc
}

func NewTaskDispatchingItemReportTo(node *jadesdk.Node, pod *kernel.Pod) *TaskDispatchingItemReportTo {
	minimumPod := pod 
	if pod != nil {
		minimumPod = pod.CopyForReportTo()
	}
	reportTo := &TaskDispatchingItemReportTo{
		Pod: minimumPod,
		Node: node,
	}
	return reportTo
}

type TaskDispatchingItemBudget struct {
	FanoutTable []float64 `json:"fanoutTable,omitempty"`
	MaximumMillisecondsInQueue float64 `json:"maximumMilliseconds,omitempty"`
}

type TaskDispatchingItemSLO struct {
	TailLatencyInMilliseconds float64 `json:"tailLatencyInMilliseconds,omitempty"`
}

func (t *TaskDispatchingItem) SetReportToForModule(moduleName string, node *jadesdk.Node, pod *kernel.Pod) {
	if t == nil || len(moduleName) == 0 {
		return
	}
	if t.ReportTo == nil {
		t.ReportTo = make(map[string]*TaskDispatchingItemReportTo)
	}
	t.ReportTo[moduleName] = NewTaskDispatchingItemReportTo(node, pod)
}

func (t *TaskDispatchingItem) GetReportToForModule(moduleName string) *TaskDispatchingItemReportTo {
	if t == nil || len(moduleName) == 0 || t.ReportTo == nil {
		return nil
	}
	if reportTo, ok := t.ReportTo[moduleName]; ok {
		return reportTo
	}
	return nil
}

func (t *TaskDispatchingItem) DescribeReportTo() string {
	if t == nil {return "nil"}

	if t.ReportTo == nil {return "nil"}

	desc := "[dispatching item]"
	for key, r := range t.ReportTo {
		desc = fmt.Sprintf("%v [module %v]: %v", desc, key, r.Desc())
	}
	return desc
}

func (t *TaskDispatchingItem) GetBudgetForModule(moduleName string) float64 {
	if t.Budgets == nil || len(t.Budgets) == 0 {
		return 0
	}
	if budgetItem, e := t.Budgets[moduleName]; e {
		return budgetItem.MaximumMillisecondsInQueue
	}
	return 0
}

func (t *TaskDispatchingItem) SetBudgetForModule(moduleName string, budget float64) {
	if t.Budgets == nil {
		t.Budgets = make(map[string]*TaskDispatchingItemBudget)
	}
	t.Budgets[moduleName] = &TaskDispatchingItemBudget{
		MaximumMillisecondsInQueue: budget,
	}
}

func (t *TaskDispatchingItem) GetBudgetForModuleAtFanoutDegree(moduleName string, fanoutDegree int) float64 {
	if t.Budgets == nil || len(t.Budgets) == 0 {
		return 0
	}
	if budgetItem, e := t.Budgets[moduleName]; e {
		if len(budgetItem.FanoutTable) < fanoutDegree+1 {
			return 0
		}
		return budgetItem.FanoutTable[fanoutDegree]
	}
	return 0
}

func (t *TaskDispatchingItem) GetDeterministicBudget(moduleName string) float64 {
	if t.Budgets == nil || len(t.Budgets) == 0 {
		return 0
	}
	if budgetItem, e := t.Budgets[moduleName]; e {
		return budgetItem.MaximumMillisecondsInQueue
	}
	return 0
}

// Status

type TaskStatus string

const (
	TaskStatusAccepted TaskStatus = "accepted"
	TaskStatusRejected            = "rejected"
	TaskStatusDone                = "done"
	TaskStatusRunning             = "running"
	TaskStatusAggregatorReady     = "aggregator_ready"
	TaskStatusWorkerReady         = "worker_ready"
	TaskStatusPending             = "pending"
	TaskStatusFailed              = "failed"
	TaskStatusInvalid             = "invalid"
)

type ObjWithTaskStatus struct {
	status TaskStatus
}

// utils
func checkStatus(selfStatus TaskStatus, cache []*ObjWithTaskStatus) TaskStatus {
	result := selfStatus
	if selfStatus != TaskStatusDone &&
		selfStatus != TaskStatusFailed &&
		selfStatus != TaskStatusRejected &&
		selfStatus != TaskStatusInvalid {
		// Need thread-safe read-lock
		accepted, taskDone, rejected, taskFailed := true, true, false, false
		for _, item := range cache {
			if item.status != TaskStatusAccepted {
				accepted = false
			}
			if item.status != TaskStatusDone {
				taskDone = false
			}
			if item.status == TaskStatusRejected {
				rejected = true
				taskDone = false
				accepted = false
			}
			if item.status == TaskStatusFailed {
				taskDone = false
				taskFailed = true
			}
		}
		if taskDone {
			result = TaskStatusDone
		} else if taskFailed {
			result = TaskStatusFailed
		} else if accepted {
			result = TaskStatusAccepted
		} else if rejected {
			result = TaskStatusRejected
		} else {
			result = TaskStatusRunning
		}
	}
	return result
}
