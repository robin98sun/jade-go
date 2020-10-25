package kernel

import (
//
)

type TaskCache struct {
	cache map[string]*taskCacheTaskItem
}

func (c *TaskCache) Describe() map[string]interface{} {
	if c == nil {
		return nil
	}
	cache := make(map[string]interface{})
	for key, item := range c.cache {
		cache[key] = item.describe()
	}
	return cache
}

func (c *TaskCache) GetTask(taskID string) *Task {
	if taskID == "" {
		return nil
	}
	if item, exists := c.cache[taskID]; exists {
		return item.task
	}
	return nil
}

type taskCacheTaskItem struct {
	task            *Task
	dispatchedNodes map[string]*taskCacheNodeItem
	status          TaskStatus
}

func (i *taskCacheTaskItem) describe() map[string]interface{} {
	if i == nil {
		return nil
	}
	dispatchedNodes := make(map[string]interface{})
	for key, node := range i.dispatchedNodes {
		dispatchedNodes[key] = node.describe()
	}
	desc := map[string]interface{}{
		"task":            i.task,
		"dispatchedNodes": dispatchedNodes,
		"status":          i.status,
	}
	return desc
}

type taskCacheNodeItem struct {
	node     *Node
	subtasks map[string]*taskCacheSubtaskItem
	status   TaskStatus
}

func (i *taskCacheNodeItem) describe() map[string]interface{} {
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

type objWithTaskStatus struct {
	status TaskStatus
}

func checkStatus(selfStatus TaskStatus, cache []*objWithTaskStatus) TaskStatus {
	result := selfStatus
	if selfStatus != TaskStatusDone &&
		selfStatus != TaskStatusFailed &&
		selfStatus != TaskStatusRejected &&
		selfStatus != TaskStatusInvalid {
		// Need thread-safe read-lock
		accepted, taskDone, rejected, taskFailed := true, true, false, false
		for _, item := range cache {
			if item.status != TaskStatusAccepted {
				accepted = false
			}
			if item.status != TaskStatusDone {
				taskDone = false
			}
			if item.status == TaskStatusRejected {
				rejected = true
				taskDone = false
				accepted = false
			}
			if item.status == TaskStatusFailed {
				taskDone = false
				taskFailed = true
			}
		}
		if taskDone {
			result = TaskStatusDone
		} else if taskFailed {
			result = TaskStatusFailed
		} else if accepted {
			result = TaskStatusAccepted
		} else if rejected {
			result = TaskStatusRejected
		} else {
			result = TaskStatusRunning
		}
	}
	return result
}

func (n *taskCacheNodeItem) CheckStatus() TaskStatus {
	var result TaskStatus
	result = TaskStatusInvalid
	if n == nil {
		return result
	}
	if len(n.subtasks) == 0 {
		return n.status
	}
	items := []*objWithTaskStatus{}
	for _, s := range n.subtasks {
		items = append(items, &objWithTaskStatus{status: s.status})
	}
	result = checkStatus(n.status, items)
	n.status = result
	return result
}

type taskCacheSubtaskItem struct {
	subtask *SubTask
	status  TaskStatus
	updates interface{}
}

func (i *taskCacheSubtaskItem) describe() map[string]interface{} {
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

type TaskStatus string

const (
	TaskStatusAccepted TaskStatus = "accepted"
	TaskStatusRejected            = "rejected"
	TaskStatusDone                = "done"
	TaskStatusRunning             = "running"
	TaskStatusPending             = "pending"
	TaskStatusFailed              = "failed"
	TaskStatusInvalid             = "invalid"
)

func (c *TaskCache) Delete(task *Task, node *Node) {
	if c.cache == nil || task == nil {
		return
	}
	if _, exists := c.cache[task.GetKey()]; !exists {
		return
	}
	if node == nil {
		delete(c.cache, task.GetKey())
	} else {
		item, _ := c.cache[task.GetKey()]
		if _, nodeExists := item.dispatchedNodes[node.Key()]; nodeExists {
			delete(item.dispatchedNodes, node.Key())
		}
	}
}

func (c *TaskCache) Set(task *Task, node *Node, subtask *SubTask, status TaskStatus, updates interface{}) string {
	if task == nil {
		return "invalid"
	}
	if c.cache == nil {
		c.cache = make(map[string]*taskCacheTaskItem)
	}
	taskKey := task.GetKey()
	if _, exists := c.cache[taskKey]; !exists {
		c.cache[taskKey] = &taskCacheTaskItem{
			task:            task,
			dispatchedNodes: make(map[string]*taskCacheNodeItem),
			status:          TaskStatusPending,
		}
	}
	taskItem := c.cache[taskKey]
	result := taskKey
	// if node is nil, then just cache a task for future process
	if node != nil {
		if _, exists := taskItem.dispatchedNodes[node.Key()]; !exists {
			newItem := &taskCacheNodeItem{
				node:     node,
				status:   TaskStatusPending,
				subtasks: make(map[string]*taskCacheSubtaskItem),
			}
			taskItem.dispatchedNodes[node.Key()] = newItem
		}
		nodeItem, _ := taskItem.dispatchedNodes[node.Key()]

		shouldCreateSubtask := true
		if subtask != nil {
			// update existing one
			if subtaskItem, subtaskExists := nodeItem.subtasks[subtask.GetKey()]; subtaskExists {
				subtaskItem.status = status
				subtaskItem.updates = updates
				result = subtask.GetKey()
				shouldCreateSubtask = false
			}
		}
		if shouldCreateSubtask {
			// insert a new subtask into the node:
			newSubtask := subtask
			if subtask == nil {
				newSubtask = NewSubtask(taskKey, "", node.Key())
			}
			result = newSubtask.GetKey()
			nodeItem.subtasks[newSubtask.GetKey()] = &taskCacheSubtaskItem{
				subtask: subtask,
				status:  status,
				updates: updates,
			}
		}

		// update task status
		if status == TaskStatusRejected || status == TaskStatusInvalid {
			taskItem.status = status
			nodeItem.status = status
		} else {
			nodeItem.CheckStatus()
			taskItem.CheckStatus()
		}
	} else {
		taskItem.status = status
	}

	// Check the task integrity at everytime something changes
	return result
}

// CheckTaskStatus check whether a task is totally accepted or rejected by all worker nodes, or totally done,
// return accepted/rejected/waiting/invalid/done
func (t *taskCacheTaskItem) CheckStatus() TaskStatus {
	if t == nil {
		return TaskStatusInvalid
	}
	items := []*objWithTaskStatus{}
	for _, s := range t.dispatchedNodes {
		items = append(items, &objWithTaskStatus{status: s.status})
	}
	t.status = checkStatus(t.status, items)
	return t.status
}
