package jadelet

import (
	"time"
	"sort"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jade-go/histogram"
)

func (j *JADE) routineForPodQueues(intervalMicroseconds int) {
	for {
		time.Sleep(time.Duration(intervalMicroseconds) * time.Millisecond)
		j.PodCache.LockMeta()
		if j.PodCache == nil || len(j.PodCache.QueuingPods) == 0 {
			j.PodCache.UnlockMeta()
			continue
		}
		startTime := time.Now()
		for _, pod := range j.PodCache.QueuingPods {
			if j.PodCache.IsPodIdle(pod) {
				go j.dispatchSubtask(pod)
				time.Sleep(time.Duration(intervalMicroseconds) * time.Microsecond)
			}
		}
		endTime := time.Now()
		duration := endTime.Sub(startTime)
		if duration/time.Millisecond > 10 {
			j.log.Printf("[pod queue routine] WARNING: checking pod queues in {%v}milliseconds", duration/time.Millisecond)
		}
		j.PodCache.UnlockMeta()
	}
}

func (j *JADE) dispatchSubtask(pod *kernel.Pod) {
	if !j.PodCache.IsPodIdle(pod) {
		j.log.Printf("ERROR when dispatching subtask to pod[%v]: the pod is busy", pod.GetKey())
		return
	}
	podCacheItem := j.PodCache.SetPodBusy(pod)
	queueItem := podCacheItem.Queue.Dequeue(j.log.Printf)
	if queueItem == nil {
		j.PodCache.SetPodIdle(pod, float64(-1), float64(-1))
		return
	}
	req := queueItem.Payload
	j.log.Printf("[task dispatcher] dispatching subtask "+pod.ModuleName+" to pod{%v [%v:%v]}: %v", pod.GetKey(), pod.Addr, pod.Port, req)
	
	inQueueTime := j.TaskCache.DispatchedPodQueueItem(pod, queueItem, time.Now())
	if inQueueTime >= 0 {
		podCacheItem.Queue.Lock()
		podCacheItem.Queue.HistogramCommunicationTime.Enqueue(inQueueTime, 1)
		podCacheItem.Queue.Unlock()
	}
	
	workerSubtaskCacheItem := j.TaskCache.GetSubtaskItem(queueItem.TaskKey, queueItem.SubtaskKey)

	_, reqlen, _, _ := j.HTTPCommunicate(
		"dispatch subtask "+string(kernel.AppModuleWorker), "POST", "/"+string(kernel.AppModuleWorker),
		pod.GetNodeRepresentation(j.Config.SelfNode.Protocol),
		req,
		0, 10,
	)

	if workerSubtaskCacheItem != nil {
		workerSubtaskCacheItem.SendPackageSize = reqlen
	}
}

func (j *JADE) checkTaskStatus(taskKey string) {
	// j.Lock()
	// defer j.Unlock()
	if j.TaskCache.CheckTask(taskKey, scheduler.TaskStatusAccepted, time.Now(), j.log.Printf) {
		j.log.Printf("[task dispatcher] the task{%v} is accepted", taskKey)
		// set the task as running
		// at the meanwhile the task record the timestamp as the beginning of ddispatching
		j.TaskCache.SetTaskStatus(taskKey, scheduler.TaskStatusRunning)
		// dispatching the task
		taskItem := j.TaskCache.GetTask(taskKey, true)
		task := taskItem.Task
		// 1. dispatch the task to the aggregator,
		//    to inform the aggregator which workers it has to wait for responses
		//   a. collect the workers
		aggregatorSubtasks := j.TaskCache.GetSubtasksRegardingNode(taskKey, string(kernel.AppModuleAggregator), "", j.Config.SelfNode.Key())
		if len(aggregatorSubtasks) > 0 {
			// only for valid aggregative tasks
			workerSubtasks := j.TaskCache.GetSubtasksRegardingNode(taskKey, "all", j.Config.SelfNode.Key(), "")
			j.log.Printf("[task dispatcher] found %v subtasks: [%v]", len(workerSubtasks), workerSubtasks)
			for _, aggregator := range aggregatorSubtasks {
				msg := NewAggregatorEnqueuingMessage(taskItem, workerSubtasks, j.Config.SelfNode.Protocol)
				msg.SubtaskKey = aggregator.Subtask.GetKey()
				j.log.Println("[task dispatcher] dispatching aggregator tasks to pod", aggregator.Subtask.Pod.GetKey())
				// Save the dispatching timestamp and fanout degree
				aggregator.Subtask.Fanout = len(workerSubtasks)
				aggregatorSubtaskCacheItem := j.TaskCache.GetSubtaskItem(taskKey, aggregator.Subtask.GetKey())
				if aggregatorSubtaskCacheItem != nil {
					aggregatorSubtaskCacheItem.EnqueueTimestamp = time.Now()
					aggregatorSubtaskCacheItem.DispatchTimestamp = time.Now()
				}
				// dispatch the aggregator subtask
				_, reqlen, _, _ := j.HTTPCommunicate(
					"dispatch subtask "+string(kernel.AppModuleAggregator), "PUT", "/$jade$/enqueueAggregativeTask",
					aggregator.Subtask.Pod.GetNodeRepresentation(j.Config.SelfNode.Protocol),
					msg,
					0, 10,
				)
				if aggregatorSubtaskCacheItem != nil {
					aggregatorSubtaskCacheItem.SendPackageSize = reqlen
				}
			}
			// 2. dispatch the subtask to each worker,
			//    together with the aggregator's address
			fanoutDegree := len(workerSubtasks)
			j.TaskCache.SetFanoutDegree(taskKey, int64(fanoutDegree))
			j.log.Printf("[task dispatcher] task[%v] fanout degree: %v", task.GetKey(), fanoutDegree)

			
			priority := taskItem.Priority
			if priority == 0 {
				priority = scheduler.TaskDefaultPriority
			}

			// calc 99 percentile for prod of histograms 
			budget := float64(0)
			if task.QueuingMechanism == kernel.TaskQueuingDDL {
				if taskItem.SLO != nil && taskItem.SLO.TailLatency99InMilliseconds > 0 {
					j.log.Printf("[task dispatcher] task SLO: %v", taskItem.SLO)
					j.log.Printf("[task dispatcher] going to calculate tail latency")
					
					histogram_list := []*histogram.Histogram{}
					for _, subtaskOnNode := range workerSubtasks {
						podQueue := j.PodCache.GetPodQueue(subtaskOnNode.Subtask.Pod)
						histogram_list = append(histogram_list, podQueue.HistogramServiceTime)
					}
					j.log.Printf("[task dispatcher] calculating tail latency using product of %v histograms", len(histogram_list))
					j.PodCache.LockData()
					tail_latency := histogram.CalcPercentileOfProduct(float64(0.99), histogram_list, false)
					j.PodCache.UnlockData()
					j.log.Printf("[task dispatcher] tail latency of %v histograms is %v", len(histogram_list), tail_latency)
					

					if tail_latency > 0 {
						budget = taskItem.SLO.TailLatency99InMilliseconds - tail_latency
						j.log.Printf("[task dispatcher] task[%v] budget calculated from online histograms: %v, where tail latency for fanout[%v]: %v", 
							task.GetKey(), budget, fanoutDegree, tail_latency)
					}
				} 
			}
			if task.QueuingMechanism == kernel.TaskQueuingDDL || task.QueuingMechanism == kernel.TaskQueuingClass {
				if budget == float64(0) {
					j.log.Printf("[task dispatcher] checking task fanout table for budget sepcification")
					budget = taskItem.GetBudgetForModuleAtFanoutDegree(string(kernel.AppModuleWorker), fanoutDegree)
					if budget > 0 {
						j.log.Printf("[task dispatcher] task[%v] budget sepcified in the task for fanout degree[%v]: %v", task.GetKey(), fanoutDegree, budget)
					} else {
						budget = taskItem.GetDeterministicBudget(string(kernel.AppModuleWorker))
						j.log.Printf("[task dispatcher] task[%v] budget sepcified in the task regardless of fanout degree: %v", task.GetKey(), budget)
					}
				}
				j.log.Printf("[task dispatcher] budget evaluation is done")
			}

			// sort available subnodes if needed
			if taskItem.Options != nil && taskItem.Options.SortSubnodes {
				sort.Slice(workerSubtasks, func(i, j int) bool {
					if workerSubtasks[i].Subtask.Pod.GetKey() < workerSubtasks[j].Subtask.Pod.GetKey() {
						return true
					}
					return i < j
				})
			}
			// j.TaskCache.SetTaskStatus(taskKey, scheduler.TaskStatusAggregatorReady)
			j.TaskCache.SetTaskTimestamp(taskKey, scheduler.TaskStatusAggregatorReady)

			// 2022-02-19
			// get online-statistics for the selected worker pods

			// end of online-statistics

			// enqueue each worker subtask
			for i, worker := range workerSubtasks {
				if worker.Subtask.ModuleName != string(kernel.AppModuleWorker) {
					continue
				}
				j.log.Printf("[task dispatcher] enqueuing subtask for pod[%v] on node[%v], which is going to report to {%v}",
					worker.Subtask.Pod.GetKey(), worker.Node.Key(),
					taskItem.GetReportToForModule(string(kernel.AppModuleWorker)),
				)
				// backdoor for fake service time
				estimatedServiceTime := float64(-1)
				j.log.Printf("[task dispatcher][debugging] options: [%v], EstimatedServiceTimeModel: [%v]", taskItem.Options, taskItem.Options.EstimatedServiceTimeModel)

				if taskItem.Options != nil && taskItem.Options.EstimatedServiceTimeModel != "" {
					options := taskItem.Options
					if options.EstimatedServiceTimeModel == "poission" {
						if options.EstimatedMeanServiceTime > 0 {
							estimatedServiceTime = float64(j.dist.PoissonRand(float64(options.EstimatedMeanServiceTime)))
						}
					} else if options.EstimatedServiceTimeModel == "exponential" {
						if options.EstimatedMeanServiceTime > 0 {
							estimatedServiceTime = float64(j.dist.ExponentialRand(float64(options.EstimatedMeanServiceTime)))
						}
					} else if options.EstimatedServiceTimeModel == "constant" && options.EstimatedMeanServiceTime > 0 {
						estimatedServiceTime = float64(options.EstimatedMeanServiceTime)
					} else if options.EstimatedServiceTimeModel == "custom" && i < len(options.ServiceTimeList) {
						estimatedServiceTime = float64(options.ServiceTimeList[i])
						j.log.Printf("[task dispatcher][debugging] using [%v]th slot (value=%v) in the service time list for pod[%v] on node[%v]", i, estimatedServiceTime, worker.Subtask.Pod.GetKey(), worker.Node.Key())
					}
				}
				// generate request payload for the subtask
				req := NewAggregativeWorkerTask(
					taskItem, worker, j.Config.SelfNode.Protocol,
					task.Application.GetModule(string(kernel.AppModuleWorker)).Input,
					estimatedServiceTime,
				)
				queue := j.PodCache.GetPodQueue(worker.Subtask.Pod)
				if queue == nil {
					j.log.Printf("[task dispatcher] ERROR when enqueuing subtask for pod[%v]: queue does not exist", worker.Subtask.Pod.GetKey())
					continue
				}
				// enqueue the subtask
				if estimatedServiceTime > 0 {
					j.log.Printf("[task dispatcher] estimated service time: [%v], according to [%v] service time distribution model",
						estimatedServiceTime, taskItem.Options.EstimatedServiceTimeModel,
					)
				}
				done,_,_ := queue.Enqueue(
					worker.Subtask.GetKey(), taskKey, worker.Subtask.GetKey(), req,
					task.QueuingMechanism, budget, priority,
					estimatedServiceTime,
					j.log.Printf,
				)
				if done {
					j.log.Printf("[task dispatcher] pod[%v] enqueued subtask[%v] for [%v] queueing", worker.Subtask.Pod.GetKey(), worker.Subtask.GetKey(), task.QueuingMechanism)
				} else {
					j.log.Printf("[task dispatcher] ERROR: failed to enqueue subtask[%v] in pod[%v]", worker.Subtask.GetKey(), worker.Subtask.Pod.GetKey())
				}
			}
			j.TaskCache.SetTaskTimestamp(taskKey, scheduler.TaskStatusWorkerReady)
			// it will fail if it has chance to fail
			// the status was set after the message is sent
			// that make it possible that the message arrives the destination
			// before the status was changed
			// even possible that the whole task is finished before the status was changed
			// so that the tasks completed extremely fast would got overwritten status back to incomplete
			// j.TaskCache.SetTaskStatus(taskKey, scheduler.TaskStatusWorkerReady)
		}
	}
}

type AggregatorEnqueuingMessage struct {
	TaskKey    string           `json:"taskId,omitempty"`
	SubtaskKey string           `json:"subtaskId,omitempty"`
	Subtasks   []string         `json:"subtasks,omitempty"`
	ReportTo   []*InterfaceSpec `json:"reportTo,omitempty"`
}

func NewAggregatorEnqueuingMessage(taskItem *scheduler.TaskDispatchingItem, subtasks []*scheduler.SubtaskOnNode, protocol string) *AggregatorEnqueuingMessage {
	inst := &AggregatorEnqueuingMessage{
		TaskKey:  taskItem.Task.GetKey(),
		Subtasks: []string{},
		ReportTo: []*InterfaceSpec{},
	}
	reportTo := taskItem.GetReportToForModule(string(kernel.AppModuleAggregator))
	if reportTo != nil && reportTo.Pod != nil {
		inst.ReportTo = append(inst.ReportTo, &InterfaceSpec{
			Node: &NodeSpec{
				Addr:     reportTo.Pod.Addr,
				Port:     reportTo.Pod.Port,
				Protocol: protocol,
			},
			ModuleName: string(kernel.AppModuleAggregator),
		})
	}
	for _, item := range subtasks {
		inst.Subtasks = append(inst.Subtasks, item.Subtask.GetKey())
	}

	return inst
}

func NewAggregativeWorkerTask(
	taskItem *scheduler.TaskDispatchingItem,
	worker *scheduler.SubtaskOnNode,
	protocol string, input interface{}, estimatedServiceTime float64,
) *Request {
	task := taskItem.Task
	reportTo := taskItem.GetReportToForModule(string(kernel.AppModuleWorker))
	if reportTo == nil || reportTo.Pod == nil {
		return nil
	}
	req := &Request{
		Task: &TaskSpec{
			ModuleName: kernel.AppModuleWorker,
			TaskID:     task.GetKey(),
			SubtaskID:  worker.Subtask.GetKey(),
		},
		To: []*InterfaceSpec{
			&InterfaceSpec{
				Node: &NodeSpec{
					Addr:     reportTo.Pod.Addr,
					Port:     reportTo.Pod.Port,
					Protocol: protocol,
				},
				ModuleName: string(kernel.AppModuleAggregator),
			},
		},
		Payload: input,
		Options: &RequestOptions{
			EstimatedServiceTime: estimatedServiceTime,
		},
	}
	return req
}

type TaskSpec struct {
	ModuleName string `json:"moduleName,omitempty"`
	TaskID     string `json:"taskId,omitempty"`
	SubtaskID  string `json:"subtaskId,omitempty"`
	TTL        int    `json:"ttl,omitempty"` // in milliseconds
}

type NodeSpec struct {
	Addr     string `json:"addr,omitempty"`
	Port     int    `json:"port,omitempty"`
	Protocol string `json:"protocol,omitempty"`
}

type InterfaceSpec struct {
	Node       *NodeSpec `json:"node,omitempty"`
	ModuleName string    `json:"moduleName,omitempty"`
}

type RequestOptions struct {
	EstimatedServiceTime float64 `json:"estimatedServiceTime,omitempty"`
}

// Request message of request
type Request struct {
	Task    *TaskSpec        `json:"task,omitempty"`
	From    *InterfaceSpec   `json:"from,omitempty"`
	To      []*InterfaceSpec `json:"to,omitempty"`
	Payload interface{}      `json:"payload,omitempty"`
	Options *RequestOptions  `json:"options,omitempty"`
}
