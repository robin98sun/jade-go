package scheduler

import (
	"aces/jade-go/kernel"
)

// the shape of task cache:
// for each task, there should have a cache for each sub-node it has been dispatched
// [task-key]: {
// 		task: task-instance,
//    status: task-status,
// 		[node-key]: {
// 			node: node-instance,
// 			status: task-status,
// 			[sub-task-key]: {
// 				subtask: sub-task-instance,
// 				status: sub-task-status,
// 				updates: sub-task-result,
// 			}
// 		}
// }

// the responsibility of task cache is:
// to retain a task after it has been dispatched to sub-nodes, and wait for response from them
// after all subnodes have accepted the task, and return the corresponding pod for that subtask
// the task cache then could dispatch the task to an aggregator, which is a task coordinator
// after dispatching pods in sub-nodes to the aggregator(task coordinator)
// the sub-tasks could be enqueued into the pod-queue for that sub-task

// when a sub-task is done, it will update it's result into the task cache

type TaskCache struct {
	Cache map[string]*TaskCacheTaskItem
}

func NewTaskCache() *TaskCache {
	inst := &TaskCache{
		Cache: make(map[string]*TaskCacheTaskItem),
	}
	return inst
}

func (c *TaskCache) Describe() map[string]interface{} {
	if c == nil {
		return nil
	}
	cache := make(map[string]interface{})
	for key, item := range c.Cache {
		cache[key] = item.describe()
	}
	return cache
}

func (c *TaskCache) GetTask(taskID string) *kernel.Task {
	if taskID == "" {
		return nil
	}
	if item, exists := c.Cache[taskID]; exists {
		return item.task
	}
	return nil
}

type TaskCacheTaskItem struct {
	task            *kernel.Task
	dispatchedNodes map[string]*TaskCacheNodeItem // node-key : nodeItem
	status          TaskStatus
}

func (i *TaskCacheTaskItem) describe() map[string]interface{} {
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

// interfaces

func (c *TaskCache) delete(task *kernel.Task, node *kernel.Node) {
	if c.Cache == nil || task == nil {
		return
	}
	if _, exists := c.Cache[task.GetKey()]; !exists {
		return
	}
	if node == nil {
		delete(c.Cache, task.GetKey())
	} else {
		item, _ := c.Cache[task.GetKey()]
		if _, nodeExists := item.dispatchedNodes[node.Key()]; nodeExists {
			delete(item.dispatchedNodes, node.Key())
		}
	}
}

func (c *TaskCache) set(task *kernel.Task, node *kernel.Node, subtask *kernel.SubTask, status TaskStatus, updates interface{}) bool {
	if task == nil {
		return false
	}
	if c.Cache == nil {
		c.Cache = make(map[string]*TaskCacheTaskItem)
	}
	taskKey := task.GetKey()
	if _, exists := c.Cache[taskKey]; !exists {
		c.Cache[taskKey] = &TaskCacheTaskItem{
			task:            task,
			dispatchedNodes: make(map[string]*TaskCacheNodeItem),
			status:          TaskStatusPending,
		}
	}
	taskItem := c.Cache[taskKey]
	result := false
	// if node is nil, then just cache a task for future process
	if node != nil {
		if _, exists := taskItem.dispatchedNodes[node.Key()]; !exists {
			newItem := &TaskCacheNodeItem{
				node:     node,
				status:   TaskStatusPending,
				subtasks: make(map[string]*TaskCacheSubtaskItem),
			}
			taskItem.dispatchedNodes[node.Key()] = newItem
		}
		nodeItem, _ := taskItem.dispatchedNodes[node.Key()]

		if subtask != nil {
			if subtaskItem, subtaskExists := nodeItem.subtasks[subtask.GetKey()]; subtaskExists {
				subtaskItem.status = status
				subtaskItem.updates = updates
			} else {
				nodeItem.subtasks[subtask.GetKey()] = &TaskCacheSubtaskItem{
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
			result = true
		}
	} else {
		taskItem.status = status
	}

	// Check the task integrity at everytime something changes
	return result
}

// CheckTaskStatus check whether a task is totally accepted or rejected by all worker nodes, or totally done,
// return accepted/rejected/waiting/invalid/done
func (t *TaskCacheTaskItem) CheckStatus() TaskStatus {
	if t == nil {
		return TaskStatusInvalid
	}
	items := []*ObjWithTaskStatus{}
	for _, s := range t.dispatchedNodes {
		items = append(items, &ObjWithTaskStatus{status: s.status})
	}
	t.status = checkStatus(t.status, items)
	return t.status
}
