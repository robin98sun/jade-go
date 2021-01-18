package scheduler

import (
	"strconv"
	"time"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jadesdk"
)

func (c *TaskCache) GetJobIdList() []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	jobs := []string{}
	for _, taskItem := range c.Cache {
		jobs = append(jobs, taskItem.task.Task.JobKey)
	}
	return jobs
}

// traceType: full / concise; jobKey: the id of which job you want to fetch, "" for all
func (c *TaskCache) CollectTraces(traceType string, jobKey string) [][]string {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	traces := [][]string{}
	headline := []string{}
	if traceType == "full" {
		headline = append(headline, "Job_ID", "Task_Status", "Task_ID", "Fanout_Degree", "Subtask_Status", "Subtask_ID", "Module_Name", "Node_ID", "Pod_ID")
		headline = append(headline, "Task_Arrival_Timestamp", "Task_Start_Timestamp", "Task_Finish_Timestamp")
		headline = append(headline, "Subtask_Arrival_Timestamp", "Subtask_Enqueue_Timestamp", "Subtask_Dispatch_Timestamp", "Subtask_Finish_Timestamp")
	} else if traceType == "concise" {
		headline = append(headline, "Job_ID")
		headline = append(headline, "Task_Index")
		headline = append(headline, "Fanout_Degree")
		headline = append(headline, "Module_Name")
		headline = append(headline, "Task_Arrival_Timestamp")
		headline = append(headline, "Subtask_Arrival_Timestamp")
	}
	headline = append(headline, "Task_Total_Time(ms)", "Task_Provision_Time(ms)", "Task_Execution_Time(ms)")
	headline = append(headline, "Subtask_Request_Time(ms)", "Subtask_Queueing_Time(ms)")
	headline = append(headline, "Queue_Length")
	headline = append(headline, "Subtask_Service_Time(ms)")
	headline = append(headline, "Subtask_Round_Trip_Time(ms)", "Subtask_Upward_Trip_Time(ms)")
	headline = append(headline, "Subtask_Downward_Package_Size", "Subtask_Upward_Package_Size")
	traces = append(traces, headline)
	taskIndex := -1
	for _, taskItem := range c.Cache {
		taskIndex++
		for _, dispatchedNode := range taskItem.dispatchedNodes {
			for _, moduleItem := range dispatchedNode.modules {
				for _, subtaskItem := range moduleItem.subtasks {
					if jobKey != "" && jobKey != "all" && jobKey != taskItem.task.Task.JobKey {
						continue
					}
					// keys
					line := []string{}
					if traceType == "full" {
						line = append(line, taskItem.task.Task.JobKey)
						line = append(line, string(taskItem.status))
						line = append(line, taskItem.task.Task.GetKey())
						line = append(line, strconv.FormatInt(taskItem.Fanout, 10))
						line = append(line, string(subtaskItem.status))
						line = append(line, subtaskItem.subtask.GetKey())
						line = append(line, subtaskItem.subtask.ModuleName)
						line = append(line, subtaskItem.subtask.NodeKey)
						line = append(line, subtaskItem.subtask.PodKey)
					} else if traceType == "concise" {
						line = append(line, taskItem.task.Task.JobKey)
						line = append(line, strconv.Itoa(taskIndex))
						line = append(line, strconv.FormatInt(taskItem.Fanout, 10))
						line = append(line, subtaskItem.subtask.ModuleName)
					}

					// timestamps
					timeArr := []time.Time{}
					if traceType == "full" {
						timeArr = append(timeArr,
							taskItem.task.GetArriveTime(),
							taskItem.DispatchTimestamp,
							taskItem.FinishTimestamp,
							subtaskItem.ArriveTimestamp,
							subtaskItem.EnqueueTimestamp,
							subtaskItem.DispatchTimestamp,
							subtaskItem.FinishTimestamp,
						)
					} else if traceType == "concise" {
						timeArr = append(timeArr,
							taskItem.task.GetArriveTime(),
							subtaskItem.ArriveTimestamp,
						)
					}
					for _, ts := range timeArr {
						if ts.IsZero() {
							line = append(line, "N/A")
						} else {
							line = append(line, strconv.FormatInt(ts.UnixNano(), 10))
						}
					}

					// task durations milliseconds
					// Task_Total_Time(ms)
					dur := int64(0)
					if !taskItem.task.GetArriveTime().IsZero() && !taskItem.FinishTimestamp.IsZero() {
						dur = int64(taskItem.FinishTimestamp.Sub(taskItem.task.GetArriveTime()) / time.Millisecond)
					}
					line = append(line, strconv.FormatInt(dur, 10))

					// Task_Provision_Time(ms)
					dur = int64(0)
					if !taskItem.task.GetArriveTime().IsZero() && !taskItem.DispatchTimestamp.IsZero() {
						dur = int64(taskItem.DispatchTimestamp.Sub(taskItem.task.GetArriveTime()) / time.Millisecond)
					}
					line = append(line, strconv.FormatInt(dur, 10))

					// Task_Execution_Time(ms)
					dur = int64(0)
					if !taskItem.DispatchTimestamp.IsZero() && !taskItem.FinishTimestamp.IsZero() {
						dur = int64(taskItem.FinishTimestamp.Sub(taskItem.DispatchTimestamp) / time.Millisecond)
					}
					line = append(line, strconv.FormatInt(dur, 10))

					// Subtask_Request_Time(ms)
					dur = int64(subtaskItem.RequestTime / time.Millisecond)
					line = append(line, strconv.FormatInt(dur, 10))
					// Subtask_Queueing_Time(ms)
					dur = int64(subtaskItem.QueueingTime / time.Millisecond)
					line = append(line, strconv.FormatInt(dur, 10))
					// Queue_Length
					line = append(line, strconv.FormatInt(subtaskItem.QueueLength, 10))
					// Subtask_Service_Time(ms)
					dur = int64(subtaskItem.ServiceTime / time.Millisecond)
					line = append(line, strconv.FormatInt(dur, 10))
					// Subtask_Round_Trip_Time(ms)
					dur = int64(subtaskItem.RTT / time.Millisecond)
					line = append(line, strconv.FormatInt(dur, 10))
					// Subtask_Upward_Trip_Time(ms)
					dur = int64(subtaskItem.ForwardingTime / time.Millisecond)
					line = append(line, strconv.FormatInt(dur, 10))
					// Subtask_Downward_Package_Size
					line = append(line, strconv.Itoa(subtaskItem.SendPackageSize))
					// Subtask_Upward_Package_Size
					line = append(line, strconv.Itoa(subtaskItem.ReceivePackageSize))
					traces = append(traces, line)
				}
			}
		}
	}
	return traces
}

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
				subtask:         subtask,
				status:          TaskStatusAccepted,
				updates:         nil,
				ArriveTimestamp: time.Now(),
			}
		}
	}
}

func (c *TaskCache) SaveResultFromApp(taskKey string, subtaskKey string, status TaskStatus, result interface{}, stat *jadesdk.StatItem) *kernel.SubTask {
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

	subtaskItem := c.Cache[taskKey].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskKey]

	subtaskItem.status = status

	if task.Options != nil && task.Options.SaveResultInCache {
		subtaskItem.updates = result
	}

	subtaskItem.FinishTimestamp = time.Now()
	subtaskItem.ForwardingTime = stat.Forwarding
	subtaskItem.ServiceTime = stat.Service
	subtaskItem.ReceivePackageSize = int(stat.PackageSize)
	subtaskItem.RequestTime = subtaskItem.FinishTimestamp.Sub(subtaskItem.DispatchTimestamp)
	subtaskItem.RTT = subtaskItem.RequestTime - subtaskItem.ServiceTime - subtaskItem.ForwardingTime
	subtaskItem.RequestTime -= subtaskItem.RTT / 2

	c.SaveStatOfModule(subtask.AppName, subtask.ModuleName, subtask.Fanout, subtaskItem)

	c.Cache[taskKey].LastUpdateTimestamp = time.Now()
	return c.Cache[taskKey].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskKey].subtask
}

func (c *TaskCache) SaveStatOfModule(
	appName string, moduleName string,
	fanoutDegree int, subtaskItem *TaskCacheSubtaskItem,
) {
	if c == nil || subtaskItem == nil {
		return
	}

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
		fanouts[fanoutKey] = jadesdk.NewStat()
	}

	stat := fanouts[fanoutKey]
	stat.PackageSize.AddNumber(int64(subtaskItem.ReceivePackageSize))
	stat.Forwarding.AddDuration(subtaskItem.ForwardingTime)
	stat.Service.AddDuration(subtaskItem.ServiceTime)
	stat.Request.AddDuration(subtaskItem.RequestTime)
	stat.RTT.AddDuration(subtaskItem.RTT)
	stat.QueueLength.AddNumber(subtaskItem.QueueLength)
	stat.QueueingTime.AddDuration(subtaskItem.QueueingTime)
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
			printf("[task cache] task[%v] is {%v}, won't check deeper", taskKey, taskItem.status)
			return taskItem.status == desiredStatus
		}
		if result := c.allSubtasksHaveTheSameStatus(taskKey, desiredStatus, printf); result {
			if desiredStatus == TaskStatusDone {
				if taskItem.FinishTimestamp.IsZero() {
					taskItem.FinishTimestamp = time.Now()
				}
			}
			return result
		}
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
		if status == TaskStatusRunning {
			if taskItem.DispatchTimestamp.IsZero() {
				taskItem.DispatchTimestamp = time.Now()
			}
		} else if status == TaskStatusDone || status == TaskStatusFailed {
			if taskItem.FinishTimestamp.IsZero() {
				taskItem.FinishTimestamp = time.Now()
			}
		}
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

func (c *TaskCache) SetFanoutDegree(taskKey string, fanout int64) {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if taskItem, e := c.Cache[taskKey]; e {
		taskItem.Fanout = fanout
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
		if taskItem.FinishTimestamp.IsZero() {
			taskItem.FinishTimestamp = time.Now()
		}
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

func (c *TaskCache) DispatchedPodQueueItem(pod *kernel.Pod, item *PodQueueItem) {
	if c == nil || len(c.Cache) == 0 {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if cacheItem, ok := c.Cache[item.TaskKey]; ok {
		if subtask := cacheItem.task.Task.GetSubtask(item.SubtaskKey); subtask != nil {
			if dispatchedNodeItem, exists := cacheItem.dispatchedNodes[pod.NodeKey]; exists {
				if subtaskCache, exists := dispatchedNodeItem.modules[pod.ModuleName]; exists {
					if subtaskItem, exists := subtaskCache.subtasks[item.SubtaskKey]; exists {
						subtaskItem.EnqueueTimestamp = item.ArrivalTime
						if subtaskItem.DispatchTimestamp.IsZero() {
							subtaskItem.DispatchTimestamp = item.DispatchTime
						}
						subtaskItem.QueueingTime = item.DispatchTime.Sub(item.ArrivalTime)
						subtaskItem.QueueLength = item.QueueLength
					}
				}
			}
		}
	}
}

func (c *TaskCache) GetSubtaskItem(taskKey string, subtaskKey string) *TaskCacheSubtaskItem {
	if c == nil || len(c.Cache) == 0 {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if cacheItem, ok := c.Cache[taskKey]; ok {
		if subtask := cacheItem.task.Task.GetSubtask(subtaskKey); subtask != nil && subtask.Pod != nil {
			if dispatchedNodeItem, exists := cacheItem.dispatchedNodes[subtask.Pod.NodeKey]; exists {
				if subtaskCache, exists := dispatchedNodeItem.modules[subtask.Pod.ModuleName]; exists {
					if subtaskItem, exists := subtaskCache.subtasks[subtask.GetKey()]; exists {
						return subtaskItem
					}
				}
			}
		}
	}
	return nil
}
