package jadelet

import (
	"aces/jade-go/kernel"
	"strings"
)

func (j *JADE) processAggregator(
	task *kernel.Task,
	targetNodes map[string][]*kernel.Task,
	masterNode *kernel.Node,
) map[string]string {

	return j.prvisionAggregator(task, targetNodes, masterNode)
}

func (j *JADE) prvisionAggregator(
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
		j.log.Println("task", task.GetKey(), "cannot perform on this node due to lacking suitable subnodes")
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
			j.log.Println("task", task.GetKey(), "cannot perform on this node due to not all target sub-nodes have available resources")
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
			subtaskKey := j.taskCache.Set(task, j.GetNodeInControl(nodeID), task.NewSubtask("worker", nodeID), kernel.TaskStatusPending, nil)
			newTask := task.CopyForSubtask()
			newTask.SubtaskKey = subtaskKey
			newTask.MasterNode = j.Config.SelfNode.MiniNode()
			targetNodes[nodeID] = append(targetNodes[nodeID], newTask)
			subtasks = append(subtasks, subtaskKey)
			result[nodeID] = subtaskKey
		}
		// 2. actually deploy aggregator on this node
		subtaskKey := j.taskCache.Set(task, j.Config.SelfNode.MiniNode(), task.NewSubtask("aggregator", j.Config.SelfNode.Key()), kernel.TaskStatusAccepted, nil)
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
		}, map[string]string{
			"name":  "JADE_MODULE",
			"value": "aggregator",
		})
		_, nodePort, err := j.Provisioner.ProvisionTask(
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
			j.log.Println("failed to provision aggregator on this node for task:", task.GetKey(), ", error:", err.Error())
		}
	}
	return nil
}
