package scheduler

import (
	"strconv"
	"time"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jadesdk"
)

func (c *TaskCache) CacheTaskForSubnode(taskKey string, subnode *kernel.Node, moduleName string, taskItem *TaskDispatchingItem, pod *kernel.Pod) {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.Cache == nil {
		c.Cache = make(map[string]*TaskCacheTaskItem)
	}
	if _, e := c.Cache[taskKey]; !e {
		if taskItem != nil && taskKey == taskItem.Task.GetKey() {
			c.Cache[taskKey] = NewTaskCacheTaskItem(taskItem)
		} else {
			return
		}
	}
	if _, e := c.Cache[taskKey].dispatchedNodes[subnode.Key()]; !e {
		c.Cache[taskKey].dispatchedNodes[subnode.Key()] = &TaskCacheNodeItem{
			node:    subnode,
			modules: make(map[string]*TaskCacheModuleItem),
			status:  TaskStatusPending,
		}
	}
	if _, e := c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName]; !e {
		if taskItem == nil {
			return
		}
		c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName] = &TaskCacheModuleItem{
			subtasks: nil,
			status:   TaskStatusPending,
		}
	}
	if pod != nil {
		var subtask *kernel.SubTask
		if len(c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks) > 0 {
			for _, tmpst := range c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks {
				if tmpst.subtask.PodKey == pod.GetKey() {
					subtask = tmpst.subtask
					subtask.Pod = pod
					tmpst.status = TaskStatusAccepted
					break
				}
			}
		}
		if subtask == nil {
			subtask := c.Cache[taskKey].task.Task.NewSubtask(
				moduleName,
				subnode.Key(),
				pod.GetKey(),
			)
			subtask.Pod = pod
			if c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks == nil {
				c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks = make(map[string]*TaskCacheSubtaskItem)
			}
			c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks[subtask.GetKey()] = &TaskCacheSubtaskItem{
				subtask: subtask,
				status:  TaskStatusAccepted,
				updates: nil,
			}
		}
	}
}

func (c *TaskCache) SaveResultFromApp(taskKey string, subtaskKey string, status TaskStatus, result interface{}) *kernel.SubTask {
	if c == nil {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := c.GetTask(taskKey)
	if task == nil {
		return nil
	}
	subtask := task.Task.GetSubtask(subtaskKey)
	if subtask == nil {
		return nil
	}
	c.Cache[taskKey].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskKey].status = status
	c.Cache[taskKey].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskKey].updates = result
	c.Cache[taskKey].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskKey].subtask.FinishTimestamp = time.Now()
	return c.Cache[taskKey].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskKey].subtask
}

type TaskResult struct {
	Status TaskStatus  `json:"status,omitempty"`
	Result interface{} `json:"result,omitempty"`
}

func (c *TaskCache) GetResultOfTask(taskKey string, moduleName string) []*TaskResult {
	if c == nil {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := c.GetTask(taskKey)
	if task == nil {
		return nil
	}
	result := []*TaskResult{}
	for _, nodeItem := range c.Cache[taskKey].dispatchedNodes {
		if moduleItem, ok := nodeItem.modules[moduleName]; ok && len(moduleItem.subtasks) > 0 {
			for _, subtaskItem := range moduleItem.subtasks {
				result = append(result, &TaskResult{
					Status: subtaskItem.status,
					Result: subtaskItem.updates,
				})
			}
		}

	}
	return result
}

func (c *TaskCache) allSubtasksHaveTheSameStatus(taskKey string, desiredStatus TaskStatus, printf func(string, ...interface{})) bool {
	if taskItem, e := c.Cache[taskKey]; e {
		if printf != nil {
			printf("[task cache] checking if all subtasks are {%v} of task[%v]", desiredStatus, taskKey)
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
							printf("[task cache] task[%v] module[%v] on node[%v] is {%v}", taskKey, moduleName, nodeItem.node.Key(), desiredStatus)
						}
					} else {
						if printf != nil {
							printf("[task cache] task[%v] module[%v] on node[%v] is NOT {%v}", taskKey, moduleName, nodeItem.node.Key(), desiredStatus)
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
				printf("[task cache] task[%v] is {%v}", taskKey, desiredStatus)
			}
			return true
		}
		if printf != nil {
			printf("[task cache] task[%v] is NOT {%v}", taskKey, desiredStatus)
		}
	}
	return false
}

func (c *TaskCache) CheckTask(taskKey string, desiredStatus TaskStatus, printf func(string, ...interface{})) bool {
	if c == nil {
		return false
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskKey]; e {
		if taskItem.status == TaskStatusRejected ||
			taskItem.status == TaskStatusDone ||
			taskItem.status == TaskStatusFailed {
			return taskItem.status == desiredStatus
		}
		return c.allSubtasksHaveTheSameStatus(taskKey, desiredStatus, printf)
	}
	return false
}

func (c *TaskCache) SetTaskStatus(taskKey string, status TaskStatus) {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskKey]; e {
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

func (c *TaskCache) RejectTask(taskKey string) {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskKey]; e {
		taskItem.status = TaskStatusRejected
	}
}

func (c *TaskCache) FailTask(taskKey string) {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskKey]; e {
		taskItem.status = TaskStatusFailed
	}
}

type SubtaskOnNode struct {
	Subtask *kernel.SubTask
	Node    *kernel.Node
}

func (c *TaskCache) GetSubtasks(taskKey string, moduleName string) []*SubtaskOnNode {
	if c == nil || c.Cache == nil {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	subtasks := []*SubtaskOnNode{}
	if taskItem, e := c.Cache[taskKey]; e {
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

func (c *TaskCache) DispatchedSubtask(taskKey string, subtaskKey string) {
	if c == nil || len(c.Cache) == 0 {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if cacheItem, ok := c.Cache[taskKey]; ok {
		subtask := cacheItem.task.Task.GetSubtask(subtaskKey)
		if subtask != nil {
			subtask.DispatchTimestamp = time.Now()
		}
	}
}

func (c *TaskCache) SaveStatOfModule(appName string, moduleName string, fanoutDegree int, statItem *jadesdk.StatItem, dispatchTimestamp time.Time, finishTimestamp time.Time) {
	if c == nil || statItem == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, ok := c.Stat[appName]; !ok {
		c.Stat[appName] = make(map[string]map[string]*jadesdk.Stat)
	}
	appItem := c.Stat[appName]

	if _, ok := appItem[moduleName]; !ok {
		appItem[moduleName] = make(map[string]*jadesdk.Stat)
	}
	fanouts := appItem[moduleName]

	realFanoutDegree := fanoutDegree
	if realFanoutDegree <= 0 {
		realFanoutDegree = 1
	}
	fanoutKey := strconv.Itoa(realFanoutDegree)
	if _, ok := fanouts[fanoutKey]; !ok {
		fanouts[fanoutKey] = &jadesdk.Stat{}
	}

	stat := fanouts[fanoutKey]
	totalDuration := finishTimestamp.Sub(dispatchTimestamp)
	stat.Total.AddDuration(totalDuration)
	onFlyDuration := totalDuration - statItem.Decoding - statItem.Forwarding - statItem.Task
	stat.OnFly.AddDuration(onFlyDuration)
	stat.ApplyItem(statItem)
}
