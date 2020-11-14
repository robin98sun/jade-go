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

func (c *TaskCache) allSubtasksHaveTheSameStatus(taskId string, desiredStatus TaskStatus, printf func(string, ...interface{})) bool {
	if taskItem, e := c.Cache[taskId]; e {
		if printf != nil {
			printf("[task cache] checking if all subtasks are {%v} of task[%v]", desiredStatus, taskId)
		}
		checkResult := desiredStatus
		for _, nodeItem := range taskItem.dispatchedNodes {
			checkNode := desiredStatus
			if len(nodeItem.modules) == 0 {
				checkResult = TaskStatusInvalid
			} else {
				for moduleName, moduleItem := range nodeItem.modules {
					checkModule := desiredStatus
					if len(moduleItem.subtasks) == 0 {
						checkModule = TaskStatusInvalid
						checkNode = TaskStatusInvalid
					} else {
						for _, subtaskItem := range moduleItem.subtasks {
							if subtaskItem.status != desiredStatus {
								checkModule = TaskStatusInvalid
								checkNode = TaskStatusInvalid
								checkResult = TaskStatusInvalid
								break
							}
						}
					}
					if checkModule == desiredStatus {
						moduleItem.status = desiredStatus
						if printf != nil {
							printf("[task cache] task[%v] module[%v] on node[%v] is {%v}", taskId, moduleName, nodeItem.node.Key(), desiredStatus)
						}
					} else {
						if printf != nil {
							printf("[task cache] task[%v] module[%v] on node[%v] is NOT {%v}", taskId, moduleName, nodeItem.node.Key(), desiredStatus)
						}
						checkNode = TaskStatusInvalid
						checkResult = TaskStatusInvalid
					}
				}
			}
			if checkNode == desiredStatus {
				nodeItem.status = desiredStatus
			} else {
				checkResult = TaskStatusInvalid
			}
		}
		if checkResult == desiredStatus {
			taskItem.status = desiredStatus
			if printf != nil {
				printf("[task cache] task[%v] is {%v}", taskId, desiredStatus)
			}
			return true
		}
		if printf != nil {
			printf("[task cache] task[%v] is NOT {%v}", taskId, desiredStatus)
		}
	}
	return false
}

func (c *TaskCache) CheckTask(taskId string, desiredStatus TaskStatus, printf func(string, ...interface{})) bool {
	if c == nil {
		return false
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskId]; e {
		if taskItem.status == TaskStatusRejected ||
			taskItem.status == TaskStatusDone ||
			taskItem.status == TaskStatusFailed {
			return taskItem.status == desiredStatus
		}
		return c.allSubtasksHaveTheSameStatus(taskId, desiredStatus, printf)
	}
	return false
}

func (c *TaskCache) SetTaskStatus(taskId string, status TaskStatus) {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskId]; e {
		taskItem.status = status
		for _, nodeItem := range taskItem.dispatchedNodes {
			nodeItem.status = status
			for _, moduleItem := range nodeItem.modules {
				moduleItem.status = status
				for _, subtaskItem := range moduleItem.subtasks {
					subtaskItem.status = status
				}
			}
		}
	}
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
