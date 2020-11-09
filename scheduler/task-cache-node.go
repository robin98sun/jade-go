package scheduler

import (
	"aces/jade-go/kernel"
)

type TaskCacheNodeItem struct {
	node     *kernel.Node
	subtasks map[string]*TaskCacheSubtaskItem
	status   TaskStatus
}

func (i *TaskCacheNodeItem) describe() map[string]interface{} {
	if i == nil {
		return nil
	}
	subtasks := make(map[string]interface{})
	for key, item := range i.subtasks {
		subtasks[key] = item.describe()
	}
	desc := map[string]interface{}{
		"node":     i.node,
		"subtasks": subtasks,
		"status":   i.status,
	}
	return desc
}

func (n *TaskCacheNodeItem) CheckStatus() TaskStatus {
	var result TaskStatus
	result = TaskStatusInvalid
	if n == nil {
		return result
	}
	if len(n.subtasks) == 0 {
		return n.status
	}
	items := []*ObjWithTaskStatus{}
	for _, s := range n.subtasks {
		items = append(items, &ObjWithTaskStatus{status: s.status})
	}
	result = checkStatus(n.status, items)
	n.status = result
	return result
}
