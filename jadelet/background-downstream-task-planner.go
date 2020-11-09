package jadelet

import (
	"aces/jade-go/kernel"
	"aces/jade-go/scheduler"
	"strconv"
)

func (j *JADE) evaluateTasks(tasklist map[string]*scheduler.TaskDispatchingItem) {
	aggregativeTasks := map[string]*scheduler.TaskDispatchingItem{}
	for taskKey, taskItem := range tasklist {
		task := taskItem.Task
		if _, aggregatorExists := task.Application.Modules[string(kernel.AppModuleAggregator)]; aggregatorExists {
			if _, workerExists := task.Application.Modules[kernel.AppModuleWorker]; workerExists {
				aggregativeTasks[taskKey] = taskItem
			}
		}
	}
	if len(aggregativeTasks) > 0 {
		j.evaluateAggregativeTasks(aggregativeTasks)
	}
}

func (j *JADE) dispatchTasks(nodeID string, tasksToDispatch []*scheduler.TaskDispatchingItem) {
	node := j.GetNodeInControl(nodeID)
	payload := j.GeneratePayloadOfRequest(node, tasksToDispatch, nil, nil)
	j.log.Println("dispatching tasks to node", nodeID)
	go j.HTTPCommunicate("dispatch tasks", "POST", "/$jade$/taskReceiver", node, payload, 0, 10)
}

func (j *JADE) newEnv(task *kernel.Task, masterNode *kernel.Node) []map[string]string {
	envVars := []map[string]string{
		map[string]string{
			"name":  "JADE_AGGREGATORNODE_ADDR",
			"value": task.Application.GetModule(string(kernel.AppModuleAggregator)).Addr,
		}, map[string]string{
			"name":  "JADE_AGGREGATORNODE_PORT",
			"value": strconv.Itoa(task.Application.GetModule(string(kernel.AppModuleAggregator)).Port),
		}, map[string]string{
			"name":  "JADE_AGGREGATORNODE_PROTOCOL",
			"value": task.Application.GetModule(string(kernel.AppModuleAggregator)).Protocol,
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

func (j *JADE) selectAvaiableNodes(requirements *kernel.Requirements) []string {
	var capableNodes []string
	if len(requirements.Exclusive) > 0 {
		capableNodes = j.capabilityCache.SelectNodesExclusively(requirements.Exclusive, nil)
	}
	if len(requirements.Exclusive) > 0 && len(capableNodes) > 0 || len(requirements.Exclusive) == 0 {
		capableNodes = j.capabilityCache.SelectNodesCollectively(requirements.Collective, capableNodes)
	}
	return capableNodes
}
