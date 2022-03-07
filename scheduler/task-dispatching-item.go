package scheduler

import (
	"time"
	"uta.edu/aces/jade-go/kernel"
)

const TaskDefaultPriority = 1000
type TaskDispatchingItem struct {
	Task            *kernel.Task                            `json:"task,omitempty"`
	ReportTo        map[string]*TaskDispatchingItemReportTo `json:"reportTo,omitempty"` // moduleName: reportTo
	SLO 			*TaskDispatchingItemSLO 				`json:"slo,omitempty"`
	Budgets         map[string]*TaskDispatchingItemBudget   `json:"budgets,omitempty"`  // moduleName: budget
	Options         *TaskDispatchingOptions                 `json:"options,omitempty"`
	Priority	    int                                     `json:"priority,omitempty"`
	arriveTimestamp time.Time
}

func (t *TaskDispatchingItem) Copy(withReport bool) *TaskDispatchingItem {
	inst := &TaskDispatchingItem{}
	if t.Task != nil {
		inst.Task = t.Task
	}
	if t.Budgets != nil {
		inst.Budgets = t.Budgets
	}
	if t.Options != nil {
		inst.Options = t.Options
	}
	inst.arriveTimestamp = t.arriveTimestamp
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
	if !t.arriveTimestamp.IsZero() {
		inst.arriveTimestamp = t.arriveTimestamp
	}
	return inst
}

func (t *TaskDispatchingItem) CopyForSubtask(withReport bool) *TaskDispatchingItem {
	inst := t.Copy(withReport)
	if inst.Task != nil {
		inst.Task = inst.Task.CopyForSubtask()
	}
	return inst
}

func (t *TaskDispatchingItem) Arrived() {
	if t != nil {
		t.arriveTimestamp = time.Now()
	}
}

func (t *TaskDispatchingItem) GetArriveTime() time.Time {
	return t.arriveTimestamp
}

type TaskDispatchingOptions struct {
	SaveResultInCache         	bool   `json:"saveResultInCache,omitempty"`
	PersistCache              	bool   `json:"persistCache,omitempty"`
	EstimatedServiceTimeModel 	string `json:"estimatedServiceTimeModel,omitempty"` // "exponential"/"poission", "constant"
	EstimatedMeanServiceTime  	float64  `json:"estimatedMeanServiceTime,omitempty"`  // for "exponential" / "poission"
	ServiceTimeList        		[]float64 `json:"serviceTimeList,omitempty"` // in milliseconds
	SortSubnodes 				bool `json:"sortSubnodes,omitempty"` // whether sort the available subnodes
}

type TaskDispatchingItemReportTo struct {
	Node *kernel.Node `json:"node,omitempty"`
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

func NewTaskDispatchingItemReportTo(node *kernel.Node, pod *kernel.Pod) *TaskDispatchingItemReportTo {
	return &TaskDispatchingItemReportTo{
		Node: node,
		Pod:  pod,
	}
}

type TaskDispatchingItemBudget struct {
	FanoutTable []float64 `json:"fanoutTable,omitempty"`
	MaximumMillisecondsInQueue float64 `json:"maximumMilliseconds,omitempty"`
}

type TaskDispatchingItemSLO struct {
	TailLatency99InMilliseconds float64 `json:"tailLatency99InMilliseconds,omitempty"`
}

func (t *TaskDispatchingItem) SetReportToForModule(moduleName string, node *kernel.Node, pod *kernel.Pod) {
	if t == nil || node == nil || pod == nil || len(moduleName) == 0 {
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
