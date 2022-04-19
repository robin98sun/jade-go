package jadelet

import (
	"strconv"

	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler"
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

func (j *JADE) newEnv(masterNode *kernel.Node, appName string, appVersion string, moduleName string, taskKey string) []map[string]string {
	envVars := []map[string]string{
		{
			"name":  "JADE_APP_NAME",
			"value": appName,
		},
		{
			"name":  "JADE_APP_VERSION",
			"value": appVersion,
		},
		{
			"name":  "JADE_APP_MODULE",
			"value": moduleName,
		},
		{
			"name":  "JADE_MASTERNODE_ADDR",
			"value": masterNode.Address,
		},
		{
			"name":  "JADE_MASTERNODE_PORT",
			"value": strconv.Itoa(masterNode.Port),
		},
		{
			"name":  "JADE_MASTERNODE_PROTOCOL",
			"value": masterNode.Protocol,
		},
		{
			// to force the k3s to truely re-provision a container
			"name":  "JADE_PROVISIONING_TASK",
			"value": taskKey,
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

func (j *JADE) selectAvaiableNodes(nodeType JadeNodeType, requirements *kernel.Requirements) []string {
	var capableNodes []string
	capabilityCache := j.subnodeCapabilityCache
	capacityCache := j.subnodeCapabilityCache
	if nodeType == JadeNodeTypeNeighbor {
		capabilityCache = j.neighborCapabilityCache
		capacityCache = j.neighborCapabilityCache
	}
	if len(requirements.Exclusive) > 0 {
		capableNodes = capabilityCache.SelectNodesExclusively(requirements.Exclusive, nil)
	}
	if len(requirements.Exclusive) > 0 && len(capableNodes) > 0 || len(requirements.Exclusive) == 0 {
		capableNodes = capacityCache.SelectNodesCollectively(requirements.Collective, capableNodes)
	}
	return capableNodes
}
