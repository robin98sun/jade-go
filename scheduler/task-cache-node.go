package scheduler

import (
	ds "uta.edu/aces/jadesdk/data_structure"
)

type TaskCacheNodeItem struct {
	node    *ds.Node
	modules map[string]*TaskCacheModuleItem
	status  TaskStatus
}

func (i *TaskCacheNodeItem) describe() map[string]interface{} {
	if i == nil {
		return nil
	}
	modules := make(map[string]interface{})
	for key, item := range i.modules {
		modules[key] = item.describe()
	}
	desc := map[string]interface{}{
		"node":    i.node,
		"modules": modules,
		"status":  i.status,
	}
	return desc
}

func (n *TaskCacheNodeItem) CheckStatus() TaskStatus {
	var result TaskStatus
	result = TaskStatusInvalid
	if n == nil {
		return result
	}
	if len(n.modules) == 0 {
		return n.status
	}
	items := []*ObjWithTaskStatus{}
	for _, s := range n.modules {
		items = append(items, &ObjWithTaskStatus{status: s.status})
	}
	result = checkStatus(n.status, items)
	n.status = result
	return result
}
