package scheduler

import (
	// "strconv"
	"time"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jadesdk"
)

func (c *TaskCache) GetJobIdList() []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	jobs := []string{}
	jobCache := make(map[string]bool)
	for _, taskItem := range c.Cache {
		if _, e := jobCache[taskItem.task.Task.JobKey]; !e {
			jobs = append(jobs, taskItem.task.Task.JobKey)
			jobCache[taskItem.task.Task.JobKey] = true
		}
	}
	return jobs
}

func (c *TaskCache) CacheTaskForSubnode(taskKey string, subnode *kernel.Node, realModuleName string, taskItem *TaskDispatchingItem, pod *kernel.Pod, originalModuleName string, subtaskKey string, printf func(string, ...interface{})) *kernel.SubTask {
	if c == nil {
		return nil
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
			return nil
		}
	}
	subnodeKey := "N/A"
	if subnode != nil {
		subnodeKey = subnode.GetKey()
	}
	podKey := "N/A"
	if pod != nil {
		podKey = pod.GetKey()
	}
	printf("[task cache] caching subtask [%v] for task [%v] on node [%v] as module [%v] which original module was [%v] in pod [%v]", subtaskKey, taskKey, subnodeKey, realModuleName, originalModuleName, podKey)
	if _, e := c.Cache[taskKey].dispatchedNodes[subnode.Key()]; !e {
		c.Cache[taskKey].dispatchedNodes[subnode.Key()] = &TaskCacheNodeItem{
			node:    subnode,
			modules: make(map[string]*TaskCacheModuleItem),
			status:  TaskStatusPending,
		}
	}
	moduleName := realModuleName
	if _, e := c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName]; !e {
		if originalModuleName == "" || realModuleName == originalModuleName {
			if taskItem == nil {
				return nil
			}
		}
		c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName] = &TaskCacheModuleItem{
			subtasks: nil,
			status:   TaskStatusPending,
		}
		printf("[task cache] created module [%v] for task [%v] on node [%v]", moduleName, taskKey, subnodeKey)
	}
	
	var subtask *kernel.SubTask
	if pod != nil {
		if originalModuleName != "" && originalModuleName != realModuleName {
			if _, e := c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[originalModuleName]; e {
				if len(c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[originalModuleName].subtasks) == 0 {
					delete(c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules, originalModuleName)
					printf("[task cache] deleted original module [%v] for task [%v] on node [%v]", originalModuleName, taskKey, subnodeKey)
				} else if _, e := c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[originalModuleName].subtasks[subtaskKey]; e {
					delete(c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[originalModuleName].subtasks, subtaskKey)
					printf("[task cache] deleted subtask [%v] from original module [%v] for task [%v] on node [%v]",subtaskKey, originalModuleName, taskKey, subnodeKey)
				}
			}
		}


		if len(c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks) > 0 {
			for _, tmpst := range c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks {
				if tmpst.subtask.PodKey == pod.GetKey() {
					subtask = tmpst.subtask
					if subtask.Pod == nil {
						subtask.Pod = pod
						tmpst.status = TaskStatusAccepted
						printf("[task cache] updated subtask [%v] in module [%v] for task [%v] on node [%v] in pod [%v]",subtaskKey, moduleName, taskKey, subnodeKey, pod.GetKey())
					}
					break
				}
			}
		}

		if subtask == nil {
			subtask = c.Cache[taskKey].task.Task.CreateSubtask(
				moduleName,
				subnode.Key(),
				pod.GetKey(),
				subtaskKey,
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

			subtaskKey = subtask.GetKey()
			printf("[task cache] created subtask [%v] in module [%v] for task [%v] on node [%v] in pod [%v]",subtaskKey, moduleName, taskKey, subnodeKey, pod.GetKey())
		}
	}
	if subtask != nil {
		subtaskKey = subtask.GetKey()
		dispatchItem := c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].subtasks[subtask.GetKey()]
		printf("[task cache] cached subtask [%v] for task [%v] on node [%v] as module [%v] which original module was [%v] in pod [%v], status: [%v]", subtaskKey, taskKey, subnodeKey, realModuleName, originalModuleName, podKey, string(dispatchItem.status))
	}


	return subtask
}

func (c *TaskCache) SaveNeighborNode(subnode *kernel.Node, taskKey string, moduleName string, neighborNode *kernel.Node, printf func(string, ...interface{})) {
	if neighborNode != nil {
		printf("[task cache] saving neighbor %v", neighborNode)
		if c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].neighbors == nil {
			c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].neighbors = make(map[string]*kernel.Node)
		}
		c.Cache[taskKey].dispatchedNodes[subnode.Key()].modules[moduleName].neighbors[neighborNode.GetKey()] = neighborNode
	}
}

func (c *TaskCache) SaveResultFromApp(taskKey string, subtaskKey string, status TaskStatus, msg *jadesdk.ReportMessage, retryCount int64, timestampReceiving time.Time,
) (*kernel.SubTask, float64, float64) {
	if c == nil {
		return nil, float64(-1), float64(-1)
	}
	result := msg.Updates 
	stat := msg.Stat
	metricsEnv := msg.MetricsEnv

	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := c.GetTask(taskKey, false)
	if task == nil {
		return nil, float64(-1), float64(-1)
	}
	subtask := task.Task.GetSubtask(subtaskKey)
	if subtask == nil {
		return nil, float64(-1), float64(-1)
	}

	subtaskItem := c.Cache[taskKey].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskKey]

	subtaskItem.status = status
	// if status != TaskStatusDone {
	// 	printf("[task cache] WARNING: update from app is not DONE but {%v} for subtask {%v} of task {%v}", status, subtaskKey, taskKey)
	// }

	if task.Options != nil && task.Options.SaveResultInCache {
		subtaskItem.updates = result
	}

	subtaskItem.FinishTimestamp = time.Now()
	if stat != nil {
		subtaskItem.ServiceTime = stat.Service
		subtaskItem.ForwardingTime = stat.Forwarding
		subtaskItem.PreServiceTime = stat.PreService
		subtaskItem.PostServiceTime = stat.PostService
		subtaskItem.ExecutionTime = stat.Execution
		subtaskItem.ReceivePackageSize = int(stat.PackageSize)
		subtaskItem.RetryCountOfSending = stat.RetryCountOfArrivalComm
	}
	subtaskItem.RetryCountOfReceiving = retryCount
	subtaskItem.ReportProcessingTime = subtaskItem.FinishTimestamp.Sub(timestampReceiving)
	
	// subtaskItem.RequestTime = subtaskItem.FinishTimestamp.Sub(subtaskItem.DispatchTimestamp) + subtaskItem.PreDispatchingTime
	subtaskItem.RequestTime = subtaskItem.FinishTimestamp.Sub(subtaskItem.DispatchTimestamp) 
	// save to histogram


	subtaskItem.CommunicationTime = subtaskItem.RequestTime - subtaskItem.ServiceTime - subtaskItem.ForwardingTime - subtaskItem.PreDispatchingTime
	subtaskItem.CommunicationTime -= subtaskItem.ReportProcessingTime
	subtaskItem.CommunicationTime -= subtaskItem.PostServiceTime
	if subtask.ModuleName == kernel.AppModuleWorker {
		subtaskItem.CommunicationTime -= subtaskItem.PreServiceTime
	}
	subtaskItem.MetricsEnv = metricsEnv

	// c.SaveStatOfModule(subtask.AppName, subtask.ModuleName, subtask.Fanout, subtaskItem)

	c.Cache[taskKey].LastUpdateTimestamp = time.Now()
	return c.Cache[taskKey].dispatchedNodes[subtask.NodeKey].modules[subtask.ModuleName].subtasks[subtaskKey].subtask, float64(subtaskItem.RequestTime)/float64(time.Millisecond), float64(subtaskItem.CommunicationTime)/float64(time.Millisecond)
}

// func (c *TaskCache) SaveStatOfModule(
// 	appName string, moduleName string,
// 	fanoutDegree int, subtaskItem *TaskCacheSubtaskItem,
// ) {
// 	if c == nil || subtaskItem == nil {
// 		return
// 	}

// 	if _, ok := c.Stat[appName]; !ok {
// 		c.Stat[appName] = make(map[string]map[string]*jadesdk.Stat)
// 	}
// 	appItem := c.Stat[appName]

// 	if _, ok := appItem[moduleName]; !ok {
// 		appItem[moduleName] = make(map[string]*jadesdk.Stat)
// 	}
// 	fanouts := appItem[moduleName]

// 	realFanoutDegree := fanoutDegree
// 	if realFanoutDegree <= 0 {
// 		realFanoutDegree = 1
// 	}
// 	fanoutKey := strconv.Itoa(realFanoutDegree)
// 	if _, ok := fanouts[fanoutKey]; !ok {
// 		fanouts[fanoutKey] = jadesdk.NewStat()
// 	}

// 	stat := fanouts[fanoutKey]
// 	stat.PreService.AddDuration(subtaskItem.PreServiceTime)
// 	stat.PackageSize.AddNumber(int64(subtaskItem.ReceivePackageSize))
// 	stat.Forwarding.AddDuration(subtaskItem.ForwardingTime)
// 	stat.Service.AddDuration(subtaskItem.ServiceTime)
// 	stat.Request.AddDuration(subtaskItem.RequestTime)
// 	stat.Communication.AddDuration(subtaskItem.CommunicationTime)
// 	stat.QueueLength.AddNumber(subtaskItem.QueueLength)
// 	stat.QueueingTime.AddDuration(subtaskItem.QueueingTime)
// }

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
	task := c.GetTask(taskKey, false)
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

func (c *TaskCache) allSubtasksHaveTheSameStatus(taskKey string, desiredStatus TaskStatus, printf func(string, ...interface{})) (bool, bool) {
	allSubtasksDone := false
	allWorkersDone := false
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
						if moduleName == kernel.AppModuleWorker {
							allWorkersDone = true
						}
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
			allSubtasksDone = true
		} else if printf != nil {
			printf("[task cache] task[%v] is NOT {%v}", taskKey, desiredStatus)
		}
	}
	return allSubtasksDone, allWorkersDone
}

func (c *TaskCache) CheckTask(taskKey string, desiredStatus TaskStatus, timestamp time.Time, printf func(string, ...interface{})) bool {
	if c == nil {
		return false
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskKey]; e {
		if taskItem.status == TaskStatusRejected ||
			taskItem.status == TaskStatusDone ||
			taskItem.status == TaskStatusFailed {	
			if taskItem.FinishTimestamp.IsZero() {
				taskItem.FinishTimestamp = time.Now()
			}
			printf("[task cache] task[%v] is already {%v}, stop checking subtasks", taskKey, taskItem.status)
			return taskItem.status == desiredStatus
		}
		allSubtasksDone, allWorkersDone := c.allSubtasksHaveTheSameStatus(taskKey, desiredStatus, printf);
		if allWorkersDone && desiredStatus == TaskStatusDone {
			taskItem.WorkerFinishTimestamp = time.Now()
		}
		if allSubtasksDone {
			if desiredStatus == TaskStatusDone {
				if taskItem.FinishTimestamp.IsZero() {
					taskItem.FinishTimestamp = time.Now()
				}
				if taskItem.LastSubtaskFinishTimestamp.IsZero() {
					taskItem.LastSubtaskFinishTimestamp = timestamp
				}
			} else if desiredStatus == TaskStatusAccepted {
				if taskItem.AcceptTimestamp.IsZero() {
					taskItem.AcceptTimestamp = timestamp
				}
			}
			return allSubtasksDone
		}
	} else {
		printf("[task cache] ERROR: task[%v] is not in cache", taskKey)
	}
	return false
}

// record timestamps for some status which is complicated for status sync in distributed env
func (c *TaskCache) SetTaskTimestamp(taskKey string, status TaskStatus) {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskKey]; e {
		if status == TaskStatusAggregatorReady {
			if taskItem.AggregatorReadyTimestamp.IsZero() {
				taskItem.AggregatorReadyTimestamp = time.Now()
			}
		} else if status == TaskStatusWorkerReady {
			if taskItem.WorkerReadyTimestamp.IsZero() {
				taskItem.WorkerReadyTimestamp = time.Now()
			}
		}
	}
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

func (c *TaskCache) GetSubtasksRegardingNode(taskKey string, moduleName string, exceptNodeKey string, exclusiveNodeKey string) []*SubtaskOnNode {
	if c == nil || c.Cache == nil {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	subtasks := []*SubtaskOnNode{}
	if taskItem, e := c.Cache[taskKey]; e {

		for _, nodeItem := range taskItem.dispatchedNodes {
			if exclusiveNodeKey != "" && exclusiveNodeKey != nodeItem.node.Key() {
				continue
			}
			if exceptNodeKey != "" && exceptNodeKey != "none" && exceptNodeKey == nodeItem.node.Key() {
				continue
			}
			for moduleNameInCache, moduleItem := range nodeItem.modules {
				if moduleName != "" && moduleName != "all" && moduleName != moduleNameInCache {
					continue
				}
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

func (c *TaskCache) GetNeighborNodesRegardingNode(taskKey string, moduleName string, exceptNodeKey string, exclusiveNodeKey string) map[string]*kernel.Node {
	if c == nil || c.Cache == nil {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskItem, e := c.Cache[taskKey]; e {
		for _, nodeItem := range taskItem.dispatchedNodes {
			if exclusiveNodeKey != "" && exclusiveNodeKey != nodeItem.node.Key() {
				continue
			}
			if exceptNodeKey != "" && exceptNodeKey != "none" && exceptNodeKey == nodeItem.node.Key() {
				continue
			}
			for moduleNameInCache, moduleItem := range nodeItem.modules {
				if moduleName != "" && moduleName != "all" && moduleName != moduleNameInCache {
					continue
				}
				return moduleItem.neighbors
			}
		}
	}
	return nil
}

func (c *TaskCache) DispatchedPodQueueItem(pod *kernel.Pod, item *PodQueueItem, timestampSending time.Time) float64 {
	if c == nil || len(c.Cache) == 0 {
		return float64(-1)
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if cacheItem, ok := c.Cache[item.TaskKey]; ok {
		if subtask := cacheItem.task.Task.GetSubtask(item.SubtaskKey); subtask != nil {
			if dispatchedNodeItem, exists := cacheItem.dispatchedNodes[pod.NodeKey]; exists {
				if subtaskCache, exists := dispatchedNodeItem.modules[pod.ModuleName]; exists {
					if subtaskItem, exists := subtaskCache.subtasks[item.SubtaskKey]; exists {
						subtaskItem.EnqueueTimestamp = item.ArrivalTime
						subtaskItem.DispatchTimestamp = timestampSending
						// subtaskItem.DispatchTimestamp = item.DispatchTime
						// the DispatchTime in queue means the moment the item been dequeued
						// so, item.DispatchTime actually means subtaskItem.DequeuingTimestamp
						// the ArrivalTime in queue actually means the moment the item been enqueued
						// that's also the moment the corresponding item for the subtask been created in queue
						subtaskItem.QueueingTime = item.DispatchTime.Sub(item.ArrivalTime)
						subtaskItem.QueueLength = item.QueueLength
						subtaskItem.EnqueuingOverhead = item.EnqueuingOverhead
						subtaskItem.AmountPreempted = item.AmountPreempted
						subtaskItem.Priority = item.Priority
						subtaskItem.Budget = item.Budget
						subtaskItem.PreDispatchingTime = timestampSending.Sub(item.DispatchTime)

						return float64(subtaskItem.QueueingTime)/float64(time.Millisecond)
					}
				}
			}
		}
	}
	return float64(-1)
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
