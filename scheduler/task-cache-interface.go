package scheduler

import (
	"uta.edu/aces/jade-go/kernel"
)

func (c *TaskCache) CacheTaskForSubnode(taskId string, subnode *kernel.Node, moduleName string, taskItem *TaskDispatchingItem, pod *kernel.Pod) {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.Cache == nil {
		c.Cache = make(map[string]*TaskCacheTaskItem)
	}
	if _, e := c.Cache[taskId]; !e {
		if taskItem != nil && taskId == taskItem.Task.GetKey() {
			c.Cache[taskId] = &TaskCacheTaskItem{
				task:            taskItem,
				dispatchedNodes: make(map[string]*TaskCacheNodeItem),
				status:          TaskStatusPending,
			}
		} else {
			return
		}
	}
	if _, e := c.Cache[taskId].dispatchedNodes[subnode.Key()]; !e {
		c.Cache[taskId].dispatchedNodes[subnode.Key()] = &TaskCacheNodeItem{
			node:    subnode,
			modules: make(map[string]*TaskCacheModuleItem),
			status:  TaskStatusPending,
		}
	}
	if _, e := c.Cache[taskId].dispatchedNodes[subnode.Key()].modules[moduleName]; !e {
		if taskItem == nil {
			return
		}
		c.Cache[taskId].dispatchedNodes[subnode.Key()].modules[moduleName] = &TaskCacheModuleItem{
			subtasks: nil,
			status:   TaskStatusPending,
		}
	}
	if pod != nil {
		var subtask *kernel.SubTask
		if len(c.Cache[taskId].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks) > 0 {
			for _, tmpst := range c.Cache[taskId].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks {
				if tmpst.subtask.PodKey == pod.GetKey() {
					subtask = tmpst.subtask
					subtask.Pod = pod
					tmpst.status = TaskStatusAccepted
					break
				}
			}
		}
		if subtask == nil {
			subtask := c.Cache[taskId].task.Task.NewSubtask(
				moduleName,
				subnode.Key(),
				pod.GetKey(),
			)
			subtask.Pod = pod
			if c.Cache[taskId].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks == nil {
				c.Cache[taskId].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks = make(map[string]*TaskCacheSubtaskItem)
			}
			c.Cache[taskId].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks[subtask.GetKey()] = &TaskCacheSubtaskItem{
				subtask: subtask,
				status:  TaskStatusAccepted,
				updates: nil,
			}
		}
	}
}

func (c *TaskCache) SaveResultFromApp(taskId string, subtaskId string, status TaskStatus, result interface{}) *kernel.SubTask {
	if c == nil {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := c.GetTask(taskId)
	if task == nil {
		return nil
	}
	subtask := task.Task.GetSubtask(subtaskId)
	if subtask == nil {
		return nil
	}
	c.Cache[taskId].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskId].status = status
	c.Cache[taskId].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskId].updates = result
	return c.Cache[taskId].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskId].subtask
}

func (c *TaskCache) CheckTask(taskId string) TaskStatus {
	if c == nil {
		return TaskStatusInvalid
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskId]; e {
		if taskItem.status == TaskStatusRejected ||
			taskItem.status == TaskStatusDone ||
			taskItem.status == TaskStatusFailed {
			return taskItem.status
		}
		acceptedStatus := TaskStatusAccepted
		doneStatus := TaskStatusDone
		for _, nodeItem := range taskItem.dispatchedNodes {
			acceptedStatusNode := TaskStatusAccepted
			doneStatusNode := TaskStatusDone
			for _, moduleItem := range nodeItem.modules {
				acceptedStatusModule := TaskStatusAccepted
				doneStatusModule := TaskStatusDone
				for _, subtaskItem := range moduleItem.subtasks {
					if subtaskItem.status != TaskStatusAccepted {
						acceptedStatus = TaskStatusInvalid
						acceptedStatusModule = TaskStatusInvalid
						acceptedStatusNode = TaskStatusInvalid
					}
					if subtaskItem.status != TaskStatusDone {
						doneStatus = TaskStatusInvalid
						doneStatusModule = TaskStatusInvalid
						doneStatusNode = TaskStatusInvalid
					}
				}
				if acceptedStatusModule == TaskStatusAccepted {
					moduleItem.status = TaskStatusAccepted
				} else if doneStatusModule == TaskStatusDone {
					moduleItem.status = TaskStatusDone
				}
			}
			if acceptedStatusNode == TaskStatusAccepted {
				nodeItem.status = TaskStatusAccepted
			} else if doneStatusNode == TaskStatusDone {
				nodeItem.status = TaskStatusDone
			}
		}
		if acceptedStatus == TaskStatusAccepted {
			taskItem.status = TaskStatusAccepted
		} else if doneStatus == TaskStatusDone {
			taskItem.status = TaskStatusDone
		}
		return taskItem.status
	}
	return TaskStatusInvalid
}

func (c *TaskCache) RejectTask(taskId string) {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskId]; e {
		taskItem.status = TaskStatusRejected
	}
}

func (c *TaskCache) FailTask(taskId string) {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskId]; e {
		taskItem.status = TaskStatusFailed
	}
}

type SubtaskOnNode struct {
	Subtask *kernel.SubTask
	Node    *kernel.Node
}

func (c *TaskCache) GetSubtasks(taskId string, moduleName string) []*SubtaskOnNode {
	if c == nil || c.Cache == nil {
		return nil
	}
	subtasks := []*SubtaskOnNode{}
	if taskItem, e := c.Cache[taskId]; e {
		for _, nodeItem := range taskItem.dispatchedNodes {
			if moduleItem, e := nodeItem.modules[moduleName]; e {
				for _, subtaskItem := range moduleItem.subtasks {
					subtasks = append(subtasks, &SubtaskOnNode{
						Subtask: subtaskItem.subtask,
						Node:    nodeItem.node,
					})
				}
			}
		}
	}
	if len(subtasks) == 0 {
		return nil
	}
	return subtasks
}
