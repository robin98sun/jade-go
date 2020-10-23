package jadelet

import (
	"aces/jade-go/kernel"
	"aces/jade-go/provisioner"
	"strings"
	// "aces/jade-go/kube"
	// "bytes"
	// "encoding/json"
	// "errors"
	// "github.com/ant0ine/go-json-rest/rest"
	"log"
	"strconv"
	// "net/http"
	// "time"
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

// evaluateTasks evaluate tasks and return a list of accepted task IDs
func (j *JADE) evaluateAggregativeTasks(tasklist map[string]*kernel.Task) {
	rejectedTasks := map[string][]string{}
	acceptedTasks := map[string][]string{}

	targetNodes := make(map[string][]*kernel.Task)

	for _, task := range tasklist {
		log.Println("evaluating task:", task.GetKey())
		j.taskCache.Set(task, nil, nil, kernel.TaskStatusPending, nil)
		// prepare environments
		if !j.HasUpperNode() {
			task.Application.GetModule("aggregator").Addr = j.Config.SelfNode.Address
		}

		accept := true
		selfInTarget := false
		workerTask := task
		nodeToSubtask := map[string]string{}
		// firstly deploy aggregator
		if j.IsAggregator() {
			masterNode := task.MasterNode
			if masterNode == nil || masterNode.IsAddrEmpty() {
				if j.Config.UpperNode.IsAddrEmpty() {
					masterNode = j.Config.SelfNode
				} else {
					masterNode = j.Config.UpperNode
				}
			}
			nodeToSubtask = j.processAggregator(task, targetNodes, masterNode)
			if nodeToSubtask == nil {
				accept = false
			}
			if tasklist, exists := targetNodes[j.Config.SelfNode.Key()]; exists {
				for _, t := range tasklist {
					if t.GetKey() == task.GetKey() {
						selfInTarget = true
						workerTask = t
						break
					}
				}
			}
		}
		// then deploy worker
		// notice, the aggregator could also be a worker
		if accept && j.IsWorker() || selfInTarget {
			subtask := task.GetSubtask(nodeToSubtask[j.Config.SelfNode.Key()])
			if !selfInTarget {
				subtask = nil
			}
			masterNode := task.MasterNode
			if masterNode == nil || masterNode.IsAddrEmpty() {
				if selfInTarget || j.Config.UpperNode.IsAddrEmpty() {
					masterNode = j.Config.SelfNode
				} else {
					masterNode = j.Config.UpperNode
				}
			}
			accept = j.processWorker(workerTask, subtask, masterNode)
		}
		if accept && selfInTarget {
			log.Println("accepted task as a worker:", task.GetKey())
			j.taskCache.Set(task, j.Config.SelfNode, task.GetSubtask(workerTask.SubtaskKey), kernel.TaskStatusAccepted, nil)
		} else if accept && j.IsWorker() {
			log.Println("accepted task as a worker:", task.GetKey())
			acceptedTasks[task.GetKey()] = append(acceptedTasks[task.GetKey()], task.SubtaskKey)
			j.taskCache.Set(task, nil, nil, kernel.TaskStatusAccepted, nil)
		} else if !accept {
			log.Println("rejected task:", task.GetKey())
			rejectedTasks[task.GetKey()] = append(rejectedTasks[task.GetKey()], task.SubtaskKey)
			j.taskCache.Set(task, nil, nil, kernel.TaskStatusRejected, nil)
		}
	}

	// dispatch sub tasks
	if len(targetNodes) > 0 {
		for nodeID, tasks := range targetNodes {
			if nodeID != j.Config.SelfNode.Key() {
				j.dispatchTasks(nodeID, tasks)
			}
		}
	}

	// feed back acceptances
	result := &TaskEvalResult{
		Accepted: acceptedTasks,
		Rejected: rejectedTasks,
	}
	if !result.IsEmpty() {
		j.feedbackTaskAcceptances(result)
	}

}

func (j *JADE) dispatchTasks(nodeID string, tasklist []*kernel.Task) {
	node := j.GetNodeInControl(nodeID)
	payload := j.GeneratePayloadOfRequest(node, tasklist, nil, nil)
	log.Println("dispatching tasks to node", nodeID)
	go j.HTTPCommunicate("dispatch tasks", "POST", "/$jade$/taskReceiver", node, payload, 0, 10)
}

func (j *JADE) processWorker(
	task *kernel.Task,
	existingSubtask *kernel.SubTask,
	masterNode *kernel.Node,
) bool {
	envVars := j.newEnv(task, masterNode)
	toReject := false
	// to see if self-node is capable
	isMissing := kernel.AnyCapabilityMissing(j.Config.Capabilities, task.Requirements.Exclusive)
	if isMissing {
		toReject = true
	}
	if !toReject {
		isCollective := kernel.AnyCapabilityExists(j.Config.Capabilities, task.Requirements.Collective)
		if !isCollective {
			toReject = true
		}
	}
	// to see if the task is acceptable
	if !toReject {
		if !j.CapacityStatus.RemainingCapacity.GE(task.Requirements.GetModule("worker").MinimumCapacity) || !j.CapacityStatus.MaximumCapacity.GE(task.Requirements.GetModule("worker").MaximumCapacity) {
			toReject = true
		}
	}
	// decide whether reject or provision the task
	if !toReject {
		// if the task is acceptable in worker role, save in task cache
		j.CapacityStatus.RemainingCapacity.Consume(task.Requirements.GetModule("worker").MinimumCapacity)
		var subtask *kernel.SubTask
		var subtaskID string
		if existingSubtask == nil {
			subtask = task.NewSubtask("worker")
			subtaskID = task.SubtaskKey
		} else {
			subtask = existingSubtask
			subtaskID = existingSubtask.GetKey()
		}
		j.taskCache.Set(task, j.Config.SelfNode, subtask, kernel.TaskStatusAccepted, nil)
		// Provision the task on self-node
		envVars = append(envVars, map[string]string{
			"name":  "JADE_SUBTASKID",
			"value": subtaskID,
		})
		_, _, err := provisioner.ProvisionTask(
			j.Kube, j.Config.SelfNode, envVars, task.Application,
			subtask.Module, task.Application.GetModule("worker"),
			task.Requirements.GetModule("worker"), 1,
		)
		if err != nil {
			j.taskCache.Set(task, j.Config.SelfNode, subtask, kernel.TaskStatusFailed, nil)
			log.Println("Failed to provision worker on this node for task", task.GetKey(), ", error:", err.Error())
		}
	}
	if toReject {
		// if something wrong, reject the task
		j.taskCache.Set(task, j.Config.SelfNode, nil, kernel.TaskStatusRejected, nil)
	}
	return !toReject
}

func (j *JADE) processAggregator(
	task *kernel.Task,
	targetNodes map[string][]*kernel.Task,
	masterNode *kernel.Node,
) map[string]string {
	envVars := j.newEnv(task, masterNode)
	// Check if the application already in cache

	// Check if the task already in cache

	// Select sub-nodes according to capabilities
	capableNodes := j.capabilityCache.SelectNodesExclusively(task.Requirements.Exclusive, nil)
	if len(capableNodes) > 0 {
		capableNodes = j.capabilityCache.SelectNodesCollectively(task.Requirements.Collective, capableNodes)
	}
	if len(capableNodes) == 0 {
		log.Println("task", task.GetKey(), "cannot perform on this node due to lacking suitable subnodes")
		j.taskCache.Set(task, j.Config.SelfNode, nil, kernel.TaskStatusRejected, nil)
		return nil
	}

	// check available nodes
	// Negotiate minimum resources for the mapping sub-task
	// Select an existing pod which is most close to it, and enqueu it in the pod queue
	if false {

	} else {
		// Evaluate whether the each sub-node can perform the task
		// if any of the sub node reject the task, then reject the task
		availableNodes := j.capacityCache.FilterAvailableNodes(capableNodes, task.Requirements.GetModule("worker"))
		if len(availableNodes) < len(capableNodes) {
			log.Println("task", task.GetKey(), "cannot perform on this node due to not all target sub-nodes have available resources")
			j.taskCache.Set(task, j.Config.SelfNode, nil, kernel.TaskStatusRejected, nil)
			return nil
		}
		// if all sub-nodes can perform the task, then
		// 1. prepare to dispatch to sub-nodes
		subtasks := []string{}
		result := make(map[string]string)
		for _, nodeID := range availableNodes {
			if _, exists := targetNodes[nodeID]; !exists {
				targetNodes[nodeID] = []*kernel.Task{}
			}
			subtaskKey := j.taskCache.Set(task, j.GetNodeInControl(nodeID), task.NewSubtask("worker"), kernel.TaskStatusPending, nil)
			newTask := task.CopyForSubtask()
			newTask.SubtaskKey = subtaskKey
			newTask.MasterNode = j.Config.SelfNode.MiniNode()
			targetNodes[nodeID] = append(targetNodes[nodeID], newTask)
			subtasks = append(subtasks, subtaskKey)
			result[nodeID] = subtaskKey
		}
		// 2. actually deploy aggregator on this node
		subtaskKey := j.taskCache.Set(task, j.Config.SelfNode, task.NewSubtask("aggregator"), kernel.TaskStatusAccepted, nil)
		aggregatorSubtaskKey := subtaskKey
		// aggregator should derive subtaskID from upper node
		if task.SubtaskKey != "" {
			aggregatorSubtaskKey = task.SubtaskKey
		} else {
			task.SubtaskKey = aggregatorSubtaskKey
		}
		envVars = append(envVars, map[string]string{
			"name":  "JADE_SUBTASKS",
			"value": strings.Join(subtasks, ","),
		}, map[string]string{
			"name":  "JADE_SUBTASKID",
			"value": aggregatorSubtaskKey,
		})
		_, nodePort, err := provisioner.ProvisionTask(
			j.Kube, j.Config.SelfNode, envVars, task.Application,
			"aggregator", task.Application.GetModule("aggregator"),
			task.Requirements.GetModule("aggregator"), 1,
		)
		if err == nil {
			// update the aggregator information: address and port
			task.Application.GetModule("aggregator").Addr = j.Config.SelfNode.Address
			task.Application.GetModule("aggregator").Port = nodePort
			return result
		} else {
			// reverse all the cached subtasks
			j.taskCache.Set(task, j.Config.SelfNode, task.GetSubtask(subtaskKey), kernel.TaskStatusFailed, nil)
			log.Println("failed to provision aggregator on this node for task:", task.GetKey(), ", error:", err.Error())
		}
	}
	return nil
}
