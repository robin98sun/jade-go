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
}

func (r *TaskEvalResult) IsEmpty() bool {
	return len(r.Accepted) == 0 && len(r.Rejected) == 0
}

func (r *TaskEvalResult) AppendAccepted(taskID string) {
	r.Accepted = append(r.Accepted, taskID)
}

func (r *TaskEvalResult) AppendRejected(taskID string) {
	r.Rejected = append(r.Rejected, taskID)
}

func (r *TaskEvalResult) AppendTask(taskID string, status string) {
	if status == "accepted" || status == "done" {
		r.AppendAccepted(taskID)
	} else if status == "rejected" {
		r.AppendRejected(taskID)
	}
}

// evaluateTasks evaluate tasks and return a list of accepted task IDs
func (j *JADE) evaluateTasks(tasklist []*kernel.Task) {
	var directlyRejected []string
	var directlyAccepted []string

	targetNodes := make(map[string][]*kernel.Task)

	for _, task := range tasklist {
		log.Println("evaluating task:", task.Key)
		// prepare environments
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
		// firstly deploy reducer
		if j.IsAggregator() {
			// Check if the application already in cache

			// Check if the task already in cache

			// Select sub-nodes according to capabilities
			capableNodes := j.capabilityCache.SelectNodesExclusively(task.Requirements.Exclusive, nil)
			if len(capableNodes) > 0 {
				capableNodes = j.capabilityCache.SelectNodesCollectively(task.Requirements.Collective, capableNodes)
			}
			if len(capableNodes) == 0 {
				log.Println("task", task.Key, "cannot perform on this node due to lacking suitable subnodes")
				j.taskCache.Set("", task, j.Config.SelfNode, false, true, false, nil)
				directlyRejected = append(directlyRejected, task.Key)
				continue
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
					log.Println("task", task.Key, "cannot perform on this node due to not all target sub-nodes have available resources")
					j.taskCache.Set("", task, j.Config.SelfNode, false, true, false, nil)
					directlyRejected = append(directlyRejected, task.Key)
					continue
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
				_, podIP, err := provisioner.ProvisionTask(j.Kube, j.Config.SelfNode, envVars, task.Application, task.Application.Reducer, task.Requirements.Allocations.Reducer)
				if err != nil {
					// can not provision reducer, then reject the task
					directlyRejected = append(directlyRejected, task.Key)
				} else {
					// prepare the reducer information: address and port
					task.Application.Reducer.Addr = podIP
					// 2. dispatch to sub-nodes
					for _, nodeID := range availableNodes {
						// cache the task to wait for sub-node's decision
						j.taskCache.Set("", task, j.Subnodes[nodeID], false, false, false, nil)
						// forward task to each sub-node
						if _, exists := targetNodes[nodeID]; !exists {
							targetNodes[nodeID] = []*kernel.Task{}
						}
						targetNodes[nodeID] = append(targetNodes[nodeID], task)
					}
				}
			}
		}
		// then deploy mapper
		// notice, the aggregator could also be a worker
		if j.IsWorker() {
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
				j.taskCache.Set("", task, j.Config.SelfNode, false, true, false, nil)
				directlyRejected = append(directlyRejected, task.Key)
			} else {
				// if the task is acceptable in worker role, save in task cache
				j.CapacityStatus.RemainingCapacity.Consume(task.Requirements.Allocations.Mapper.MinimumCapacity)
				j.taskCache.Set("", task, j.Config.SelfNode, true, false, false, nil)
				directlyAccepted = append(directlyAccepted, task.Key)
				// Provision the task on self-node
				if !j.Config.UpperNode.IsAddrEmpty() {
					envVars = append(envVars, map[string]string{
						"name":  "JADE_MASTERNODE_ADDR",
						"value": j.Config.UpperNode.Address,
					}, map[string]string{
						"name":  "JADE_MASTERNODE_PORT",
						"value": strconv.Itoa(j.Config.UpperNode.Port),
					}, map[string]string{
						"name":  "JADE_MASTERNODE_PROTOCOL",
						"value": j.Config.UpperNode.Protocol,
					})
				} else {
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
				}

				go provisioner.ProvisionTask(j.Kube, j.Config.SelfNode, envVars, task.Application, task.Application.Mapper, task.Requirements.Allocations.Mapper)
			}
		}
	}
	result := &TaskEvalResult{
		Accepted: directlyAccepted,
		Rejected: directlyRejected,
	}
	if !result.IsEmpty() {
		j.feedbackTaskStatus(result)
	}
	if len(targetNodes) > 0 {
		for nodeID, tasks := range targetNodes {
			j.dispatchTasks(nodeID, tasks)
		}
	}
}

func (j *JADE) dispatchTasks(nodeID string, tasklist []*kernel.Task) {
	node := j.Subnodes[nodeID]
	payload := j.GeneratePayloadOfRequest(node, tasklist, nil, nil)
	log.Println("dispatching tasks to node", nodeID)
	go j.HTTPCommunicate("dispatch tasks", "POST", "/$jade$/taskReceiver", node, payload, 0, 10)
}
