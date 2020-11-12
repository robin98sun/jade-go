package scheduler

import (
	// "aces/jade-go/kernel"
	"sync"
)

// the shape of task cache:
// for each task, there should have a cache for each sub-node it has been dispatched
// [task-key]: {
// 		task: task-instance,
//    status: task-status,
// 		[node-key]: {
// 			node: node-instance,
// 			status: task-status,
// 			[moduleName]: {
//        status: task-status,
// 			  [sub-task-key]: {
// 				  subtask: sub-task-instance,
// . 				status: sub-task-status,
//  				updates: sub-task-result,
// 	  		}
// .    }
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
	mutex *sync.Mutex
}

func NewTaskCache() *TaskCache {
	inst := &TaskCache{
		Cache: make(map[string]*TaskCacheTaskItem),
		mutex: &sync.Mutex{},
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

func (c *TaskCache) GetTask(taskID string) *TaskDispatchingItem {
	if taskID == "" {
		return nil
	}
	if item, exists := c.Cache[taskID]; exists {
		return item.task
	}
	return nil
}

type TaskCacheTaskItem struct {
	task            *TaskDispatchingItem
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
		"task":            i.task.Task,
		"dispatchedNodes": dispatchedNodes,
		"status":          i.status,
	}
	return desc
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
