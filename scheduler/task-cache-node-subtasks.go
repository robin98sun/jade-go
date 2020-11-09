package scheduler

import (
	"aces/jade-go/kernel"
)

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
