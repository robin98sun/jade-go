package jadelet

import (
	"aces/jade-go/kernel"
	"strconv"
)

type TaskEvalResult struct {
	Accepted map[string][]string `json:"accepted,omitempty"`
	Rejected map[string][]string `json:"rejected,omitempty"`
	Done     map[string][]string `json:done,omitempty`
	Failed   map[string][]string `json:error,omitempty`
}

func NewTaskEvalResult() *TaskEvalResult {
	return &TaskEvalResult{
		Accepted: map[string][]string{},
		Rejected: map[string][]string{},
		Done:     map[string][]string{},
		Failed:   map[string][]string{},
	}
}

func (r *TaskEvalResult) IsEmpty() bool {
	return len(r.Accepted)+len(r.Rejected)+len(r.Done)+len(r.Failed) == 0
}

func (r *TaskEvalResult) AppendTask(taskID string, subtaskID string, status kernel.TaskStatus) {
	if status == kernel.TaskStatusAccepted {
		r.Accepted[taskID] = append(r.Accepted[taskID], subtaskID)
	} else if status == kernel.TaskStatusRejected {
		r.Rejected[taskID] = append(r.Rejected[taskID], subtaskID)
	} else if status == kernel.TaskStatusDone {
		r.Done[taskID] = append(r.Done[taskID], subtaskID)
	} else if status == kernel.TaskStatusFailed {
		r.Failed[taskID] = append(r.Failed[taskID], subtaskID)
	}
}

func (j *JADE) evaluateTasks(tasklist map[string]*kernel.Task) {
	aggregativeTasks := map[string]*kernel.Task{}
	for taskKey, task := range tasklist {
		if _, aggregatorExists := task.Application.Modules["aggregator"]; aggregatorExists {
			if _, workerExists := task.Application.Modules["worker"]; workerExists {
				aggregativeTasks[taskKey] = task
			}
		}
	}
	if len(aggregativeTasks) > 0 {
		j.evaluateAggregativeTasks(aggregativeTasks)
	}
}

func (j *JADE) dispatchTasks(nodeID string, tasklist []*kernel.Task) {
	node := j.GetNodeInControl(nodeID)
	payload := j.GeneratePayloadOfRequest(node, tasklist, nil, nil)
	j.log.Println("dispatching tasks to node", nodeID)
	go j.HTTPCommunicate("dispatch tasks", "POST", "/$jade$/taskReceiver", node, payload, 0, 10)
}

func (j *JADE) newEnv(task *kernel.Task, masterNode *kernel.Node) []map[string]string {
	envVars := []map[string]string{
		map[string]string{
			"name":  "JADE_AGGREGATORNODE_ADDR",
			"value": task.Application.GetModule("aggregator").Addr,
		}, map[string]string{
			"name":  "JADE_AGGREGATORNODE_PORT",
			"value": strconv.Itoa(task.Application.GetModule("aggregator").Port),
		}, map[string]string{
			"name":  "JADE_AGGREGATORNODE_PROTOCOL",
			"value": task.Application.GetModule("aggregator").Protocol,
		}, map[string]string{
			"name":  "JADE_TTL",
			"value": strconv.Itoa(task.Budget.MaximumMilliseconds / 1000),
		}, map[string]string{
			"name":  "JADE_TASKID",
			"value": task.GetKey(),
		}, map[string]string{
			"name":  "JADE_MASTERNODE_ADDR",
			"value": masterNode.Address,
		}, map[string]string{
			"name":  "JADE_MASTERNODE_PORT",
			"value": strconv.Itoa(masterNode.Port),
		}, map[string]string{
			"name":  "JADE_MASTERNODE_PROTOCOL",
			"value": masterNode.Protocol,
		},
	}
	// add capabilities into environments
	for _, c := range j.Config.Capabilities {
		envVars = append(envVars, map[string]string{
			"name":  c.Name,
			"value": c.API,
		})
	}
	return envVars
}
