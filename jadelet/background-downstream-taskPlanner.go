package jadelet

import (
	"aces/jade-go/kernel"
	"aces/jade-go/provisioner"
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
	Accepted []string `json:"accepted,omitempty"`
	Rejected []string `json:"rejected,omitempty"`
	Done     []string `json:done,omitempty`
	Failed   []string `json:error,omitempty`
}

func NewTaskEvalResult() *TaskEvalResult {
	return &TaskEvalResult{
		Accepted: []string{},
		Rejected: []string{},
		Done:     []string{},
		Failed:   []string{},
	}
}

func (r *TaskEvalResult) IsEmpty() bool {
	return len(r.Accepted)+len(r.Rejected)+len(r.Done)+len(r.Failed) == 0
}

func (r *TaskEvalResult) AppendTask(taskID string, status kernel.TaskStatus) {
	if status == kernel.TaskStatusAccepted {
		r.Accepted = append(r.Accepted, taskID)
	} else if status == kernel.TaskStatusRejected {
		r.Rejected = append(r.Rejected, taskID)
	} else if status == kernel.TaskStatusDone {
		r.Done = append(r.Done, taskID)
	} else if status == kernel.TaskStatusFailed {
		r.Failed = append(r.Failed, taskID)
	}
}

// evaluateTasks evaluate tasks and return a list of accepted task IDs
func (j *JADE) evaluateTasks(tasklist map[string]*kernel.Task) {
	var rejectedTasks []string
	var acceptedTasks []string

	targetNodes := make(map[string][]*kernel.Task)

	for _, task := range tasklist {
		log.Println("evaluating task:", task.GetKey())
		j.taskCache.Set(task, nil, "", kernel.TaskStatusPending, nil)
		// prepare environments
		if !j.HasUpperNode() {
			task.Application.Reducer.Addr = j.Config.SelfNode.Address
		}
		envVars := []map[string]string{
			map[string]string{
				"name":  "JADE_REDUCERNODE_ADDR",
				"value": task.Application.Reducer.Addr,
			}, map[string]string{
				"name":  "JADE_REDUCERNODE_PORT",
				"value": strconv.Itoa(task.Application.Reducer.Port),
			}, map[string]string{
				"name":  "JADE_REDUCERNODE_PROTOCOL",
				"value": task.Application.Reducer.Protocol,
			}, map[string]string{
				"name":  "JADE_TTL",
				"value": strconv.Itoa(task.Budget.MaximumMilliseconds / 1000),
			},
		}
		// add capabilities into environments
		for _, c := range j.Config.Capabilities {
			envVars = append(envVars, map[string]string{
				"name":  c.Name,
				"value": c.API,
			})
		}
		accept := true
		// firstly deploy reducer
		if j.IsAggregator() {
			accept = j.processAggregator(task, envVars, targetNodes)
		}
		// then deploy mapper
		// notice, the aggregator could also be a worker
		if accept && j.IsWorker() {
			accept = j.processWorker(task, envVars)
		}
		if accept && !j.IsAggregator() {
			log.Println("accepted task:", task.GetKey())
			acceptedTasks = append(acceptedTasks, task.GetKey())
			j.taskCache.Set(task, nil, "", kernel.TaskStatusAccepted, nil)
		} else if !accept {
			log.Println("rejected task:", task.GetKey())
			rejectedTasks = append(rejectedTasks, task.GetKey())
			j.taskCache.Set(task, nil, "", kernel.TaskStatusRejected, nil)
		}
	}

	// dispatch sub tasks
	if len(targetNodes) > 0 {
		for nodeID, tasks := range targetNodes {
			j.dispatchTasks(nodeID, tasks)
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
	node := j.Subnodes[nodeID]
	payload := j.GeneratePayloadOfRequest(node, tasklist, nil, nil)
	log.Println("dispatching tasks to node", nodeID)
	go j.HTTPCommunicate("dispatch tasks", "POST", "/$jade$/taskReceiver", node, payload, 0, 10)
}

func (j *JADE) processWorker(
	task *kernel.Task,
	env []map[string]string,
) bool {
	envVars := env
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
		if !j.CapacityStatus.RemainingCapacity.GE(task.Requirements.Allocations.Mapper.MinimumCapacity) || !j.CapacityStatus.MaximumCapacity.GE(task.Requirements.Allocations.Mapper.MaximumCapacity) {
			toReject = true
		}
	}
	// decide whether reject or provision the task
	if toReject {
		// if something wrong, reject the task
		j.taskCache.Set(task, j.Config.SelfNode, "", kernel.TaskStatusRejected, nil)
	} else {
		// if the task is acceptable in worker role, save in task cache
		j.CapacityStatus.RemainingCapacity.Consume(task.Requirements.Allocations.Mapper.MinimumCapacity)
		j.taskCache.Set(task, j.Config.SelfNode, "", kernel.TaskStatusAccepted, nil)
		// Provision the task on self-node
		masterNode := j.Config.SelfNode
		if j.HasUpperNode() && !j.IsAggregator() {
			masterNode = j.Config.UpperNode
		}
		envVars = append(envVars, map[string]string{
			"name":  "JADE_MASTERNODE_ADDR",
			"value": masterNode.Address,
		}, map[string]string{
			"name":  "JADE_MASTERNODE_PORT",
			"value": strconv.Itoa(masterNode.Port),
		}, map[string]string{
			"name":  "JADE_MASTERNODE_PROTOCOL",
			"value": masterNode.Protocol,
		})

		go provisioner.ProvisionTask(
			j.Kube, j.Config.SelfNode, envVars, task.Application,
			"mapper", task.Application.Mapper,
			task.Requirements.Allocations.Mapper, 1,
		)
	}
	return !toReject
}

func (j *JADE) processAggregator(
	task *kernel.Task,
	env []map[string]string,
	targetNodes map[string][]*kernel.Task,
) bool {
	envVars := env
	// Check if the application already in cache

	// Check if the task already in cache

	// Select sub-nodes according to capabilities
	capableNodes := j.capabilityCache.SelectNodesExclusively(task.Requirements.Exclusive, nil)
	if len(capableNodes) > 0 {
		capableNodes = j.capabilityCache.SelectNodesCollectively(task.Requirements.Collective, capableNodes)
	}
	if len(capableNodes) == 0 {
		log.Println("task", task.GetKey(), "cannot perform on this node due to lacking suitable subnodes")
		j.taskCache.Set(task, j.Config.SelfNode, "", kernel.TaskStatusRejected, nil)
		return false
	}

	// check available nodes
	// Negotiate minimum resources for the mapping sub-task
	alloReq := task.Requirements.Allocations.Mapper
	// Select an existing pod which is most close to it, and enqueu it in the pod queue
	if false {

	} else {
		// Evaluate whether the each sub-node can perform the task
		// if any of the sub node reject the task, then reject the task
		availableNodes := j.capacityCache.FilterAvailableNodes(capableNodes, alloReq)
		if len(availableNodes) < len(capableNodes) {
			log.Println("task", task.GetKey(), "cannot perform on this node due to not all target sub-nodes have available resources")
			j.taskCache.Set(task, nil, "", kernel.TaskStatusRejected, nil)
			return false
		}
		// if all sub-nodes can perform the task, then
		envVars = append(envVars, map[string]string{
			"name":  "JADE_MASTERNODE_ADDR",
			"value": j.Config.SelfNode.Address,
		}, map[string]string{
			"name":  "JADE_MASTERNODE_PORT",
			"value": strconv.Itoa(j.Config.SelfNode.Port),
		}, map[string]string{
			"name":  "JADE_MASTERNODE_PROTOCOL",
			"value": j.Config.SelfNode.Protocol,
		})
		// 1. deploy reducer on this node
		_, nodePort, err := provisioner.ProvisionTask(
			j.Kube, j.Config.SelfNode, envVars, task.Application,
			"reducer", task.Application.Reducer,
			task.Requirements.Allocations.Reducer, 1,
		)
		accept := true
		if err != nil {
			// can not provision reducer, then reject the task
			accept = false
		} else {
			// prepare the reducer information: address and port
			task.Application.Reducer.Addr = j.Config.SelfNode.Address
			task.Application.Reducer.Port = nodePort
			// 2. dispatch to sub-nodes
			for _, nodeID := range availableNodes {
				// cache the task to wait for sub-node's decision
				j.taskCache.Set(task, j.Subnodes[nodeID], "", kernel.TaskStatusPending, nil)
				// forward task to each sub-node
				if _, exists := targetNodes[nodeID]; !exists {
					targetNodes[nodeID] = []*kernel.Task{}
				}
				targetNodes[nodeID] = append(targetNodes[nodeID], task)
			}
			j.taskCache.Set(task, j.Config.SelfNode, "", kernel.TaskStatusAccepted, nil)
		}
		return accept
	}
	return true
}
