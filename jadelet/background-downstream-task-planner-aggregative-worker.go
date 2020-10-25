package jadelet

import (
	"aces/jade-go/kernel"
)

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
			subtask = task.NewSubtask("worker", j.Config.SelfNode.Key())
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
		}, map[string]string{
			"name":  "JADE_MODULE",
			"value": "worker",
		})
		_, _, err := j.Provisioner.ProvisionTask(
			j.Kube, j.Config.SelfNode, envVars, task.Application,
			subtask.Module, task.Application.GetModule("worker"),
			task.Requirements.GetModule("worker"), 1,
		)
		if err != nil {
			j.taskCache.Set(task, j.Config.SelfNode, subtask, kernel.TaskStatusFailed, nil)
			j.log.Println("Failed to provision worker on this node for task", task.GetKey(), ", error:", err.Error())
		}
	}
	if toReject {
		// if something wrong, reject the task
		j.taskCache.Set(task, j.Config.SelfNode, nil, kernel.TaskStatusRejected, nil)
	}
	return !toReject
}
