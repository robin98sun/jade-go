package jadelet

import (
	"aces/jade-go/kernel"
	// "aces/jade-go/kube"
	// "bytes"
	// "encoding/json"
	// "errors"
	// "github.com/ant0ine/go-json-rest/rest"
	"log"
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
	for _, task := range tasklist {
		log.Println("evaluating task:", task.Key)
		if j.IsWorker() {
			// to see if self-node is capable

			// to see if the task is acceptable
			// try to provision the task

			if false {
				// if something wrong, reject the task
				j.taskCache.Set("", task, j.Config.SelfNode, false, true, false, nil)
				directlyRejected = append(directlyRejected, task.Key)
			} else {
				// if the task is acceptable in worker role, save in task cache
				j.taskCache.Set("", task, j.Config.SelfNode, true, false, false, nil)
				directlyAccepted = append(directlyAccepted, task.Key)
			}
		}
		if j.IsAggregator() {
			// Check if the application already in cache

			// Check if the task already in cache

			// Select sub-nodes according to capabilities
			capableNodes := j.capabilityCache.SelectNodes(task.Requirements.Capabilities)
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
				for _, nodeID := range availableNodes {
					// cache the task to wait for sub-node's decision
					j.taskCache.Set("", task, j.Subnodes[nodeID], false, false, false, nil)
					// negoatiate with each sub-node to allocate the task

				}
			}
		}
	}
	result := &TaskEvalResult{
		Accepted: directlyAccepted,
		Rejected: directlyRejected,
	}
	j.forwardTaskStatus(result)
}

func (j *JADE) forwardTaskStatus(evalRes *TaskEvalResult) {
	if j.HasUpperNode() {
		payload := j.GenerateUpstreamPayloadOfControlPath(evalRes, nil, nil, nil)
		go j.HTTPCommunicate("feedback task acceptances", "POST", "/$jade$/taskAcceptances", j.Config.UpperNode, payload, 0, 10)
	} else {
		// send the result to UI
	}
}
