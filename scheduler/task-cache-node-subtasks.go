package scheduler

import (
	"uta.edu/aces/jade-go/kernel"
)

type TaskCacheModuleItem struct {
	subtasks map[string]*TaskCacheSubtaskItem
	status   TaskStatus
}

func (i *TaskCacheModuleItem) describe() map[string]interface{} {
	if i == nil {
		return nil
	}
	subtasksDesc := map[string]interface{}{}
	if len(i.subtasks) == 0 {
		subtasksDesc = nil
	} else {
		for key, value := range i.subtasks {
			subtasksDesc[key] = value.describe()
		}
	}
	desc := map[string]interface{}{
		"subtasks": subtasksDesc,
		"status":   i.status,
	}
	return desc
}

type TaskCacheSubtaskItem struct {
	subtask *kernel.SubTask
	status  TaskStatus
	updates interface{}
}

func (i *TaskCacheSubtaskItem) describe() map[string]interface{} {
	if i == nil {
		return nil
	}
	desc := map[string]interface{}{
		"subtask": i.subtask,
		"status":  i.status,
		"updates": i.updates,
	}
	return desc
}
