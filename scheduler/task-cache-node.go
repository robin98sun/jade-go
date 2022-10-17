package scheduler

import (
	ds "uta.edu/aces/jadesdk/data_structure"
)

type TaskCacheNodeItem struct {
	node    *ds.Node
	modules map[string]*TaskCacheModuleItem
	status  ds.TaskStatus
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

func (n *TaskCacheNodeItem) CheckStatus() ds.TaskStatus {
	var result ds.TaskStatus
	result = ds.TaskStatusInvalid
	if n == nil {
		return result
	}
	if len(n.modules) == 0 {
		return n.status
	}
	items := []*ds.ObjWithTaskStatus{}
	for _, s := range n.modules {
		items = append(items, &ds.ObjWithTaskStatus{Status: s.status})
	}
	result = ds.CheckTaskStatus(n.status, items)
	n.status = result
	return result
}
