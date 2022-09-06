package jadelet

import (
	"time"
	"sort"
	"math"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler/task"
	"uta.edu/aces/jade-go/scheduler/histogram"
)

func (j *JADE) routineForPodQueues(intervalNanoseconds int) {
	for {
		time.Sleep(time.Duration(intervalNanoseconds) * time.Nanosecond)
		podsInCache := j.PodCache.GetPods()
		if len(podsInCache) == 0 {
			continue
		}
		startTime := time.Now()
		j.PodCache.Lock()
		for _, pod := range podsInCache {
			if j.PodCache.IsPodIdle(pod) {
				go j.dispatchSubtask(pod)
				// time.Sleep(time.Duration(intervalNanoseconds) * time.Nanosecond)
			}
		}
		j.PodCache.Unlock()

		endTime := time.Now()
		duration := endTime.Sub(startTime)
		podRoutineOverhead := math.Round(float64(duration*10/time.Millisecond))/10
		if  podRoutineOverhead > 10 {
			j.log.Perf.Printf("[pod queue routine] WARNING: checking pod queues in {%v}milliseconds", podRoutineOverhead)
		}
	}
}

func (j *JADE) dispatchSubtask(pod *kernel.Pod) {
	j.PodCache.Lock()

	if !j.PodCache.IsPodIdle(pod) {
		j.PodCache.Unlock()
		// j.log.Printf("ERROR when dispatching subtask to pod[%v]: the pod is busy", pod.GetKey())
		return
	}
	podCacheItem := j.PodCache.SetPodBusy(pod)
	queueItem := podCacheItem.Queue.Dequeue(j.log.Debug.Printf)
	if queueItem == nil {
		j.PodCache.SetPodIdle(pod, float64(-1), float64(-1), float64(-1), float64(-1))
		j.PodCache.Unlock()
		return
	}
	j.PodCache.Unlock()

	req := queueItem.Payload
	j.log.Debug.Printf("[task dispatcher] dispatching subtask "+pod.ModuleName+" to pod{%v [%v:%v]}: %v", pod.GetKey(), pod.Addr, pod.Port, req)
	
	// inQueueTime := j.TaskCache.DispatchedPodQueueItem(pod, queueItem, time.Now())
	// if inQueueTime >= 0 {
	// 	podCacheItem.Queue.Lock()
	// 	podCacheItem.Queue.HistogramCommunicationTime.Enqueue(inQueueTime, 1)
	// 	podCacheItem.Queue.Unlock()
	// }
	j.TaskCache.DispatchedPodQueueItem(pod, queueItem, time.Now())
	
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

	j.PerfCache.AppendQueueDeadlineViolationEvent(pod.NodeKey, float64(queueItem.DispatchTime.Sub(queueItem.Deadline)/time.Millisecond))
}

func (j *JADE) checkTaskStatus(taskKey string, isConfirmingBudget bool, dispatchItemToConfirm *task.TaskDispatchingItem) {
	// j.Lock()
	// defer j.Unlock()
	if isConfirmingBudget || j.TaskCache.CheckTask(taskKey, task.TaskStatusAccepted, time.Now(), j.log.Debug.Printf)  {
		if ! isConfirmingBudget {
			j.log.Debug.Printf("[task dispatcher] the task{%v} is accepted", taskKey)
		} else {
			j.log.Debug.Printf("[task dispatcher] the task{%v} is confirming budget to neighbors", taskKey)
			if dispatchItemToConfirm != nil && dispatchItemToConfirm.Options != nil {
				j.log.Debug.Printf("[task dispatcher] the incoming task non-block budget negotiation phase: %v", dispatchItemToConfirm.Options.BudgetNegotiationPhase)
			}

		}
		// set the task as running
		// at the meanwhile the task record the timestamp as the beginning of ddispatching
		if ! isConfirmingBudget {
			j.TaskCache.SetTaskStatus(taskKey, task.TaskStatusRunning)
		}
		// dispatching the task
		dispatchItem := j.TaskCache.GetTask(taskKey, true)
		task := dispatchItem.Task
		
		// 1. dispatch the task to the aggregator,
		//    to inform the aggregator which workers it has to wait for responses
		//   a. collect the workers
		aggregatorSubtasks := j.TaskCache.GetSubtasksRegardingNode(taskKey, string(kernel.AppModuleAggregator), "", j.Config.SelfNode.Key())
		if len(aggregatorSubtasks) > 0 {
			// only for valid aggregative tasks
			phase := task.BudgetNegotiationPhaseNotStarted
			budget := float64(0)
			priority := 0
			if isConfirmingBudget {
				budget = dispatchItemToConfirm.GetBudgetForModule(string(kernel.AppModuleWorker))
				priority = dispatchItemToConfirm.Priority
			} else {
				budget = dispatchItem.GetBudgetForModule(string(kernel.AppModuleWorker))
				priority = dispatchItem.Priority
			}
			if priority == 0 {
				priority = task.TaskDefaultPriority
			}
			j.log.Debug.Printf("[task dispatcher] budget: %v, priority: %v", budget, priority)

			var neighborSubtasks []*task.SubtaskOnNode
			var allSubtasks []*task.SubtaskOnNode
			var workerSubtasks []*task.SubtaskOnNode

			workerSubtasks = j.TaskCache.GetSubtasksRegardingNode(taskKey, string(kernel.AppModuleWorker), j.Config.SelfNode.Key(), "")

			budgetNegotiation := task.BudgetNegotiationTypeNone
			// if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiation != "" {
				// budgetNegotiation = dispatchItem.Options.BudgetNegotiation
			// }
			if task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block {
				budgetNegotiation = task.BudgetNegotiationTypeCDFBlock
			} else if task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
				budgetNegotiation = task.BudgetNegotiationTypeCDFNonBlock
			}
			var budgetnegotationCache *task.BudgetNegotiationResponseCache
				
			neighborNodes := j.TaskCache.GetNeighborNodesRegardingNode(taskKey, string(kernel.AppModuleAggregator), "", j.Config.SelfNode.Key())
			for _, neighborNode := range neighborNodes {
				subtaskItem := &task.SubtaskOnNode{
					Node: neighborNode,
					Subtask: kernel.NewSubtask(
						task.GetKey(),
						task.Application.Name,
						string(kernel.AppModuleAggregator),
						neighborNode.GetKey(),
						"", "",
					),
				}
				neighborSubtasks = append(neighborSubtasks, subtaskItem)

			}
			
			allSubtasks = workerSubtasks
			if len(neighborSubtasks) > 0 {
				allSubtasks = append([]*task.SubtaskOnNode{}, workerSubtasks...)
				allSubtasks = append(allSubtasks, neighborSubtasks...)
			}

			j.log.Debug.Printf("[task dispatcher] found %v internal subtasks, %v neighbor subtasks, the aggregator will be waiting for %v subtasks", len(workerSubtasks), len(neighborSubtasks), len(allSubtasks))

			for _, aggregator := range aggregatorSubtasks {
				if !isConfirmingBudget {
					msg := NewAggregatorEnqueuingMessage(dispatchItem, allSubtasks, j.Config.SelfNode.Protocol)
					msg.SubtaskKey = aggregator.Subtask.GetKey()
					j.log.Debug.Println("[task dispatcher] dispatching aggregator tasks to pod", aggregator.Subtask.Pod.GetKey())
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

				// dispatch the neighbor subtasks
				if len(neighborSubtasks) > 0 {
					j.log.Debug.Printf("[task dispatcher] dispatching neighbor subtasks")
					
					if task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock || 
						(	task.QueuingMechanism == kernel.TaskQueuingDDL && 
							budgetNegotiation == task.BudgetNegotiationTypeCDFNonBlock) {

						if !isConfirmingBudget {
							phase = task.BudgetNegotiationPhaseInquiry
						} else {
							phase = task.BudgetNegotiationPhaseConfirm
						}
						if !isConfirmingBudget {
							budgetnegotationCache = task.NewBudgetNegotiationResponseCache()
						}
					}

					for _, neighborItem := range neighborSubtasks {
						newDispatchItem := dispatchItem.CopyForSubtask(false)
						newDispatchItem.Task.SubtaskKey = neighborItem.Subtask.GetKey()

						newDispatchItem.SetReportToForModule(string(kernel.AppModuleWorker), nil, aggregator.Subtask.Pod)

						// for non-block budget negotiation, prepare for the cache
						if task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {

							if newDispatchItem.Options == nil {
								newDispatchItem.Options = &task.TaskDispatchingOptions{}
							}
							newDispatchItem.Options.BudgetNegotiationPhase = phase
							// newDispatchItem.Options.BudgetNegotiation = task.BudgetNegotiationTypeCDFNonBlock
							newDispatchItem.Options.BudgetNegotiationInitiator = j.Config.SelfNode.MiniNode()

							if !isConfirmingBudget {
								budgetnegotationCache.Responses[neighborItem.Node.Key()] = &task.BudgetNegotiationResponseCacheItem{
									Neighbor: neighborItem.Node,
									IsDone: false,
									RequestSentAt: time.Now(),
								}
							}
						}

						dispatchItem.InquiryStartTimestamp = time.Now()
						go j.dispatchNeighborTask(neighborItem.Node, newDispatchItem)
						if task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
							j.log.Debug.Printf("[task dispatcher] non-block budget negotiation phase: %v", phase)
						}
						if !isConfirmingBudget {
							j.log.Debug.Printf("[task dispatcher] initiating subtask %v for neighbor %v has been dispatched, the reportTo of the dispatching message is: %v",  neighborItem.Subtask.GetKey(), neighborItem.Node.GetKey(), newDispatchItem.DescribeReportTo())
						} else {
							j.log.Debug.Printf("[task dispatcher] confirming budget for subtask %v for neighbor %v has been dispatched, the reportTo of the dispatching message is: %v",  neighborItem.Subtask.GetKey(), neighborItem.Node.GetKey(), newDispatchItem.DescribeReportTo())

						}
					}

					// wait for the responses of budget negotiation
					if budgetnegotationCache != nil {
						j.TaskCache.SetBudgetNegotiationCache(task.GetKey(), budgetnegotationCache)
						j.log.Debug.Printf("[task dispatcher] budget negotiation cache is setup")
					}
				}
			}

			
			if !isConfirmingBudget {
				// 2. dispatch the subtask to each worker,
				//    together with the aggregator's address
				fanoutDegree := len(workerSubtasks)
				j.TaskCache.SetFanoutDegree(taskKey, int64(fanoutDegree))
				j.log.Debug.Printf("[task dispatcher] task[%v] fanout degree: %v", task.GetKey(), fanoutDegree)

				// calc 99 percentile for prod of histograms 
				j.log.Debug.Printf("[task dispatcher] queueing mechanism: %v", task.QueuingMechanism)
				if dispatchItem.SLO != nil {
					j.log.Debug.Printf("[task dispatcher] SLO: %v", dispatchItem.SLO.TailLatencyInMilliseconds)
				}
				if dispatchItem.Options != nil {
					j.log.Debug.Printf("[task dispatcher] SLO percentile: %v", dispatchItem.Options.BudgetEstimationPercentilePoint)
				}
				if task.QueuingMechanism == kernel.TaskQueuingClass {
					if dispatchItem.SLO != nil {
						budget = dispatchItem.SLO.TailLatencyInMilliseconds
					}
				} else if budget == 0 {
					// calculate budget using online measurement
					if 	task.QueuingMechanism == kernel.TaskQueuingDDL || 
						task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block ||
						task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock || 
						task.QueuingMechanism == kernel.TaskQueuingDDL_None {
						if dispatchItem.SLO != nil && dispatchItem.SLO.TailLatencyInMilliseconds > 0 {
							provisionOverhead := float64(0)
							overheadCheckpoint := time.Now()
							if !time.Time.IsZero(dispatchItem.BudgetEstimationDoneTimestamp) && dispatchItem.BudgetEstimationDoneTimestamp.Sub(dispatchItem.ArriveTimestamp) > 0 {
								provisionOverhead = float64(overheadCheckpoint.Sub(dispatchItem.BudgetEstimationDoneTimestamp)*10 / time.Millisecond)/10
							} else {
								provisionOverhead = float64(overheadCheckpoint.Sub(dispatchItem.ArriveTimestamp)*10/ time.Millisecond)/10
							}
							dispatchItem.SLO.TailLatencyInMilliseconds -= provisionOverhead
							j.log.Debug.Printf("[task dispatcher] deduct %vms provision overheads to get precise SLO %vms", provisionOverhead, dispatchItem.SLO)

							j.log.Debug.Printf("[task dispatcher] going to calculate tail latency")


							pods_to_calculate_unloaded_tail := []*kernel.Pod{}

							for _, subtaskOnNode := range workerSubtasks {
								pods_to_calculate_unloaded_tail = append(pods_to_calculate_unloaded_tail, subtaskOnNode.Subtask.Pod)
							}
							// histogram_list := []*histogram.Histogram{}
							// for _, subtaskOnNode := range workerSubtasks {
							// 	podQueue := j.PodCache.GetPodQueue(subtaskOnNode.Subtask.Pod)
							// 	histogram_list = append(histogram_list, podQueue.HistogramServiceTime)
							// }
							// j.log.Debug.Printf("[task dispatcher] calculating tail latency using product of %v histograms", len(histogram_list))
							// j.PodCache.Lock()

							budgetEstimationPercentilePoint := float64(0.99)
							if dispatchItem.Options != nil && dispatchItem.Options.BudgetEstimationPercentilePoint > 0 && dispatchItem.Options.BudgetEstimationPercentilePoint <= 1 {
								budgetEstimationPercentilePoint = dispatchItem.Options.BudgetEstimationPercentilePoint
							}
							// unloaded_tail_latency := histogram.CalcPercentileOfProduct(budgetEstimationPercentilePoint, histogram_list, false)
							// j.PodCache.Unlock()

							unloaded_tail_latency := j.PodCache.CalcTailForPods(pods_to_calculate_unloaded_tail, budgetEstimationPercentilePoint, task.PodQueueHistogramTypeServiceResponseTime)

							j.log.Debug.Printf("[task dispatcher] tail latency of %v pods at percentile point %v is %v", len(pods_to_calculate_unloaded_tail), budgetEstimationPercentilePoint, unloaded_tail_latency)
							
							// could never happen, don't know why it is here
							if unloaded_tail_latency < 0 {
								unloaded_tail_latency = 0
							}

							tailCalcOverhead := float64(time.Now().Sub(overheadCheckpoint)*10 / time.Millisecond)/10
							budget = dispatchItem.SLO.TailLatencyInMilliseconds - unloaded_tail_latency - tailCalcOverhead

							j.TaskCache.SetUnloadedTailLatencyAndBudgetForTask(dispatchItem.Task.GetKey(), unloaded_tail_latency, budget)
							j.log.Debug.Printf("[task dispatcher] task[%v] budget calculated from online histograms: %v, where tail latency for fanout[%v]: %v", 
								task.GetKey(), budget, fanoutDegree, unloaded_tail_latency)
						} 
					}
					// for queueing by class,
					//     and compatible with legacy using static budget
					if budget == 0 && task.QueuingMechanism == kernel.TaskQueuingDDL {
						j.log.Debug.Printf("[task dispatcher] checking task fanout table for budget sepcification")
						budget = dispatchItem.GetBudgetForModuleAtFanoutDegree(string(kernel.AppModuleWorker), fanoutDegree)
						if budget > 0 {
							j.log.Debug.Printf("[task dispatcher] task[%v] budget sepcified in the task for fanout degree[%v]: %v", task.GetKey(), fanoutDegree, budget)
						} else {
							budget = dispatchItem.GetDeterministicBudget(string(kernel.AppModuleWorker))
							j.log.Debug.Printf("[task dispatcher] task[%v] budget sepcified in the task regardless of fanout degree: %v", task.GetKey(), budget)
						}
					}
					j.log.Debug.Printf("[task dispatcher] budget evaluation is done")
				}


				j.log.Debug.Printf("[task dispatcher] task budget: %v", budget)
				
				// sort available subnodes if needed
				if dispatchItem.Options != nil && dispatchItem.Options.SortSubnodes {
					sort.Slice(workerSubtasks, func(i, j int) bool {
						if workerSubtasks[i].Subtask.Pod.GetKey() < workerSubtasks[j].Subtask.Pod.GetKey() {
							return true
						}
						return i < j
					})
				}
				// j.TaskCache.SetTaskStatus(taskKey, task.TaskStatusAggregatorReady)
				j.TaskCache.SetTaskTimestamp(taskKey, task.TaskStatusAggregatorReady)
			}

			// enqueue each worker subtask
			for i, worker := range workerSubtasks {
				if worker.Subtask.ModuleName != string(kernel.AppModuleWorker) {
					continue
				}
				j.log.Debug.Printf("[task dispatcher] enqueuing subtask for pod[%v] on node[%v], which is going to report to {%v}",
					worker.Subtask.Pod.GetKey(), worker.Node.Key(),
					dispatchItem.GetReportToForModule(string(kernel.AppModuleWorker)).Desc(),
				)
				// backdoor for fake service time
				estimatedServiceTime := float64(-1)
				j.log.Debug.Printf("[task dispatcher][debugging] options: [%v], EstimatedServiceTimeModel: [%v]", dispatchItem.Options, dispatchItem.Options.EstimatedServiceTimeModel)

				if dispatchItem.Options != nil && dispatchItem.Options.EstimatedServiceTimeModel != "" {
					options := dispatchItem.Options
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
						j.log.Debug.Printf("[task dispatcher][debugging] using [%v]th slot (value=%v) in the service time list for pod[%v] on node[%v]", i, estimatedServiceTime, worker.Subtask.Pod.GetKey(), worker.Node.Key())
					}
				}
				// generate request payload for the subtask
				req := NewAggregativeWorkerTask(
					dispatchItem, worker, j.Config.SelfNode.Protocol,
					task.Application.GetModule(string(kernel.AppModuleWorker)).Input,
					estimatedServiceTime,
				)
				queue := j.PodCache.GetPodQueue(worker.Subtask.Pod)
				if queue == nil {
					j.log.Debug.Printf("[task dispatcher] ERROR when enqueuing subtask for pod[%v]: queue does not exist", worker.Subtask.Pod.GetKey())
					continue
				}
				// enqueue the subtask
				if estimatedServiceTime > 0 {
					j.log.Debug.Printf("[task dispatcher] estimated service time: [%v], according to [%v] service time distribution model",
						estimatedServiceTime, dispatchItem.Options.EstimatedServiceTimeModel,
					)
				}

				// according to budget negotiation method, to enqueue the task
				budgetNegotiation := task.BudgetNegotiationTypeNone
				// if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiation != "" {
				// 	budgetNegotiation = dispatchItem.Options.BudgetNegotiation
				// }
				if task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
					budgetNegotiation = task.BudgetNegotiationTypeCDFNonBlock
				} else if task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block {
					budgetNegotiation = task.BudgetNegotiationTypeCDFBlock
				}
				
				phase := task.BudgetNegotiationPhaseNotStarted
				if isConfirmingBudget {
					phase = task.BudgetNegotiationPhaseConfirm
				} else if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiationPhase != "" {
					phase = dispatchItem.Options.BudgetNegotiationPhase
				}
				if (task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock ||
					(task.QueuingMechanism == kernel.TaskQueuingDDL && 
					  budgetNegotiation == task.BudgetNegotiationTypeCDFNonBlock)) {
					j.log.Debug.Printf("[task dispatcher] the non-block budget negotiation phase is [%v]", phase)
				}

				targetQueue := task.PodQueueTypeMain
				if isConfirmingBudget {
					targetQueue = task.PodQueueTypeMain
				} else if (task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock ||
					(task.QueuingMechanism == kernel.TaskQueuingDDL && 
					  budgetNegotiation == task.BudgetNegotiationTypeCDFNonBlock)) &&
					phase != task.BudgetNegotiationPhaseConfirm {
					targetQueue = task.PodQueueTypeShadow
				}

				queuingMech := task.QueuingMechanism
				if task.QueuingMechanism == kernel.TaskQueuingDDL {
					if budgetNegotiation == task.BudgetNegotiationTypeCDFNonBlock {
						queuingMech = kernel.TaskQueuingDDL_CDF_NonBlock
					} else if budgetNegotiation == task.BudgetNegotiationTypeCDFBlock {
						queuingMech = kernel.TaskQueuingDDL_CDF_Block
					} else if budgetNegotiation == task.BudgetNegotiationTypeNone {
						queuingMech = kernel.TaskQueuingDDL_None
					}
				}
				done,_,_ := queue.Enqueue(
					targetQueue,
					worker.Subtask.GetKey(), taskKey, worker.Subtask.GetKey(), req,
					queuingMech, budget, priority,
					estimatedServiceTime,
					j.log.Debug.Printf,
				)
				if done || isConfirmingBudget{
					if done {
						j.log.Debug.Printf("[task dispatcher] pod[%v] enqueued subtask[%v] for [%v] queueing", worker.Subtask.Pod.GetKey(), worker.Subtask.GetKey(), task.QueuingMechanism)
					} else {
						j.log.Debug.Printf("[task dispatcher] pod[%v] subtask[%v] for [%v] queueing has been served before budget negotiation is done", worker.Subtask.Pod.GetKey(), worker.Subtask.GetKey(), task.QueuingMechanism)
					}
				} else {
					j.log.Debug.Printf("[task dispatcher] ERROR: failed to enqueue subtask[%v] in pod[%v]", worker.Subtask.GetKey(), worker.Subtask.Pod.GetKey())
				}
			}
			j.TaskCache.SetTaskTimestamp(taskKey, task.TaskStatusWorkerReady)
			// it will fail if it has chance to fail
			// the status was set after the message is sent
			// that make it possible that the message arrives the destination
			// before the status was changed
			// even possible that the whole task is finished before the status was changed
			// so that the tasks completed extremely fast would got overwritten status back to incomplete
			// j.TaskCache.SetTaskStatus(taskKey, task.TaskStatusWorkerReady)

			// for non-block budget negotiation, prepare for the cache
			if !isConfirmingBudget && (task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock || 
				(	task.QueuingMechanism == kernel.TaskQueuingDDL && 
					budgetNegotiation == task.BudgetNegotiationTypeCDFNonBlock)){
				// just to collect the local cdf and check the remote CDFs
				pods := []*kernel.Pod{}
				for _, worker := range workerSubtasks {
					pods = append(pods, worker.Subtask.Pod)
				}

				if  budgetnegotationCache != nil {
					// as an initiator
					go j.CheckBudgetNegotiationCache(task.GetKey(), budgetnegotationCache, pods, dispatchItem)
				} else if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiationInitiator != nil {
					// report to the initiator
					go j.ReportCDFtoInitiator(pods, dispatchItem, dispatchItem.Options.BudgetNegotiationInitiator)
				}

			}
		}
	}
}

func (j *JADE) CheckBudgetNegotiationCache(taskId string, cache *task.BudgetNegotiationResponseCache, pods []*kernel.Pod, dispatchItem *task.TaskDispatchingItem) {
	localResponse := j.MultiplyCDFs(pods, dispatchItem)

	for {

		isDone := true

		cache.Lock()
		for _, cacheItem := range cache.Responses {
			if !cacheItem.IsDone {
				isDone = false
				break
			}
		}
		cache.Unlock()

		if isDone {
			break
		}

		time.Sleep(50000 * time.Nanosecond)
	}

	cache.Lock()

	// multiply CDFs
	cdf_list := []*histogram.CDF{ localResponse.CDF }
	for _, cacheItem := range cache.Responses {
		if cacheItem.Response == nil || cacheItem.Response.CDF == nil {
			continue
		}
		// to be simpler in research, we do not reject neighbors regarding their CDFs
		// but if in business, we should
		cdf_list = append(cdf_list, cacheItem.Response.CDF)
		dispatchItem.Task.SaveNeighborNode(cacheItem.Response.Node)
	}
	dispatchItem.InquiryDoneTimestamp = time.Now()

	j.CalcGlobalBudget(cdf_list, dispatchItem)

	cache.Unlock()

	j.log.Debug.Printf("[budget negotiation] non-block negotiation is done, going to re-dispatch the task among all eligible clusters, there are %v neighbor subtasks", len(dispatchItem.Task.NeighborNodes))

	if dispatchItem.Options == nil {
		dispatchItem.Options = &task.TaskDispatchingOptions{}
	}
	dispatchItem.Options.BudgetNegotiationPhase = task.BudgetNegotiationPhaseConfirm

	j.evaluateAggregativeTasks(map[string]*task.TaskDispatchingItem{
		dispatchItem.Task.GetKey(): dispatchItem,
	})

}

func (j *JADE) ReportCDFtoInitiator(pods []*kernel.Pod, dispatchItem *task.TaskDispatchingItem, initiator *kernel.Node) {
	localResponse := j.MultiplyCDFs(pods, dispatchItem)
	j.log.Debug.Printf("[budget negotiation] non-block negotiation going to report CDF to the initiator[%v]", initiator)
	payload := j.GeneratePayloadOfRequest(initiator, localResponse, nil, nil)
	apiPath := "/$jade$/collectCDF"
	_, _, _, err := j.HTTPCommunicate("reporting CDF to the initiator", "PUT", apiPath, initiator, payload, 0, 10)
	if err != nil {
		j.log.Debug.Println("[budget negotiation] ERROR when reporting CDF to the initiator:", err.Error())
	} 
}


type AggregatorEnqueuingMessage struct {
	TaskKey    string           `json:"taskId,omitempty"`
	SubtaskKey string           `json:"subtaskId,omitempty"`
	Subtasks   []string         `json:"subtasks,omitempty"`
	ReportTo   []*InterfaceSpec `json:"reportTo,omitempty"`
}

func NewAggregatorEnqueuingMessage(taskItem *task.TaskDispatchingItem, subtasks []*task.SubtaskOnNode, protocol string) *AggregatorEnqueuingMessage {
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
	taskItem *task.TaskDispatchingItem,
	worker *task.SubtaskOnNode,
	protocol string, input interface{}, estimatedServiceTime float64,
) *Request {
	task := taskItem.Task
	reportTo := taskItem.GetReportToForModule(string(kernel.AppModuleWorker))
	if reportTo == nil || reportTo.Pod == nil {
		return nil
	}
	req := &Request{
		Task: &TaskSpec{
			ModuleName: string(kernel.AppModuleWorker),
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
