package kernel

import (
//
)

type TaskCache struct {
	cache map[string]*taskCacheTaskItem
}
type taskCacheTaskItem struct {
	task            *Task
	dispatchedNodes map[string]*taskCacheNodeItem
	taskDone        bool
	accepted        bool
	rejected        bool
}

type taskCacheNodeItem struct {
	node     *Node
	accepted bool
	rejected bool
	taskDone bool
	result   interface{}
}

func (c *TaskCache) Set(taskID string, task *Task, node *Node, accept bool, reject bool, done bool, result interface{}) string {
	if (taskID == "" && task == nil) || node == nil {
		return "invalid"
	}
	if c.cache == nil {
		c.cache = make(map[string]*taskCacheTaskItem)
	}
	taskKey := taskID
	if task != nil {
		taskKey = task.Key
	}
	if _, exists := c.cache[taskKey]; !exists {
		item := &taskCacheTaskItem{
			dispatchedNodes: make(map[string]*taskCacheNodeItem),
			taskDone:        false,
		}
		if task != nil {
			item.task = task
		}
		c.cache[taskKey] = item
	}

	taskItem := c.cache[taskKey]
	taskItem.dispatchedNodes[node.Key()] = &taskCacheNodeItem{
		node:     node,
		accepted: accept,
		rejected: reject,
		taskDone: done,
		result:   result,
	}
	c.cache[taskKey] = taskItem
	// Check the task integrity at everytime something changes
	return c.CheckTaskStatus(taskKey)
}

// CheckTaskStatus check whether a task is totally accepted or rejected by all worker nodes, or totally done,
// return accepted/rejected/waiting/invalid/done
func (c *TaskCache) CheckTaskStatus(taskID string) string {
	if taskID == "" {
		return "invalid"
	}
	if c.cache == nil {
		return "invalid"
	}
	if item, exists := c.cache[taskID]; exists {
		if item.taskDone {
			return "done"
		} else if item.accepted {
			return "accepted"
		} else if item.rejected {
			return "rejected"
		} else {
			// Need thread-safe read-lock
			item.accepted = true
			item.taskDone = true
			item.rejected = false
			for _, nodeItem := range item.dispatchedNodes {
				if !nodeItem.accepted {
					item.accepted = false
				}
				if !nodeItem.taskDone {
					item.taskDone = false
				}
				if nodeItem.rejected {
					item.rejected = true
					break
				}
			}
			if item.taskDone {
				return "done"
			} else if item.accepted {
				return "accepted"
			} else if item.rejected {
				return "rejected"
			} else {
				return "waiting"
			}
		}
	} else {
		return "invalid"
	}
}
