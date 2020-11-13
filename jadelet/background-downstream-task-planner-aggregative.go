package jadelet

import (
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler"
)

// evaluateTasks evaluate tasks and return a list of accepted task IDs
func (j *JADE) evaluateAggregativeTasks(tasklist map[string]*scheduler.TaskDispatchingItem) {
	rejectTaskCache := make(map[string]*scheduler.TaskDispatchingItem) // taskKey: *TaskDispatchingItem
	// first, check or allocate itself's pod
	// 1. if the node itself is a coordinator, then allocate an aggregator pod for it
	goodTaskCache := make(map[string]*scheduler.TaskDispatchingItem) // taskKey: *TaskDispatchingItem
	for _, taskItem := range tasklist {
		task := taskItem.Task
		if j.IsCoordinator() {
			// allocate an aggregator pod if needed
			aggregatorAllocation := task.Requirements.Allocations[string(kernel.AppModuleAggregator)]
			aggregatorPod := j.PodCache.GetPodForApplication(j.SelfNodeKey(), task.Application, string(kernel.AppModuleAggregator), aggregatorAllocation)
			if aggregatorPod == nil {
				// provision an aggregator pod
				podName, nodePort, err := j.Provisioner.ProvisionTask(
					j.Kube, j.Config.SelfNode, j.newEnv(j.Config.SelfNode, task.Application.Name, string(kernel.AppModuleAggregator)),
					task.Application, string(kernel.AppModuleAggregator),
					task.Application.GetModule(string(kernel.AppModuleAggregator)),
					task.Requirements.GetModule(string(kernel.AppModuleAggregator)),
					1,
				)
				if err != nil {
					j.log.Println("ERROR when provisioning", string(kernel.AppModuleAggregator), "for task", task.GetKey())
				} else {
					aggregatorPod = &kernel.Pod{
						NodeKey:    j.Config.SelfNode.Key(),
						Namespace:  j.Config.SelfNode.Namespace,
						PodName:    podName,
						Addr:       j.Config.SelfNode.Address,
						Port:       nodePort,
						Allocation: task.Requirements.GetModule(string(kernel.AppModuleAggregator)),
						AppKey:     task.Application.Key(),
						Container:  task.Application.GetModule(string(kernel.AppModuleAggregator)),
						ModuleName: string(kernel.AppModuleAggregator),
					}
					aggregatorPod.GetKey()
					j.PodCache.SetPodForApplication(
						j.Config.SelfNode.Key(),
						task.Application,
						string(kernel.AppModuleAggregator),
						task.Requirements.GetModule(string(kernel.AppModuleAggregator)),
						aggregatorPod,
						false,
					)
				}
			}
			if aggregatorPod == nil {
				// reject the task
				rejectTaskCache[task.GetKey()] = taskItem
			} else {
				j.TaskCache.CacheTaskForSubnode(task.GetKey(), j.Config.SelfNode, string(kernel.AppModuleAggregator), taskItem, aggregatorPod)
				newItem := taskItem.Copy()
				newItem.ReportTo.Pod = aggregatorPod
				newItem.ReportTo.Node = j.Config.SelfNode
				goodTaskCache[task.GetKey()] = newItem
			}
		} else {
			goodTaskCache[task.GetKey()] = taskItem
		}
	}
	// second, downstream propagate the task
	if len(goodTaskCache) > 0 {
		j.downstreamPropagating(goodTaskCache)
	}
	// third, reject bad tasks
	if len(rejectTaskCache) > 0 {
		for _, taskItem := range rejectTaskCache {
			j.TaskCache.RejectTask(taskItem.Task.GetKey())
			j.feedbackProvisioning(NewTaskProvisioningResult(
				j.Config.SelfNode.Key(),
				taskItem.Task.GetKey(),
				string(kernel.AppModuleAggregator),
				nil,
			))
		}
	}
}

func (j *JADE) downstreamPropagating(tasklist map[string]*scheduler.TaskDispatchingItem) {
	tasksGoingToDispatch := make(map[string][]*scheduler.TaskDispatchingItem) // nodekey: []*TaskDispatchingItem
	readyTaskCache := make(map[string]*kernel.Pod)                            // taskkey: *Pod
	rejectTaskCache := make(map[string]*scheduler.TaskDispatchingItem)        // taskKey: *TaskDispatchingItem
	for _, taskItem := range tasklist {
		task := taskItem.Task
		j.log.Println("evaluating task:", task.GetKey())
		// 1. search all required sub-nodes
		availableNodes := []string{}
		if j.IsCoordinator() {
			availableNodes = j.selectAvaiableNodes(task.Requirements)
			j.log.Printf("found {%v} available nodes: %v", len(availableNodes), availableNodes)
		} else {
			availableNodes = []string{j.SelfNodeKey()}
		}
		// 2. for each available sub-nodes:
		for _, nodekey := range availableNodes {
			j.log.Printf("processing node[%v]", nodekey)
			//		search pod on that node for this task
			workerAllocation := task.Requirements.Allocations[string(kernel.AppModuleWorker)]
			workerPod := j.PodCache.GetPodForApplication(nodekey, task.Application, string(kernel.AppModuleWorker), workerAllocation)
			// if the node itself is also a worker, then allcate a worker pod for it
			if j.IsSelfNode(nodekey) {
				j.log.Printf("[%v] is a self-node", nodekey)
				if workerPod == nil {
					// provision a worker Pod for it
					podName, nodePort, err := j.Provisioner.ProvisionTask(
						j.Kube, j.Config.SelfNode, j.newEnv(taskItem.ReportTo.Node, task.Application.Name, string(kernel.AppModuleWorker)),
						task.Application, string(kernel.AppModuleWorker),
						task.Application.GetModule(string(kernel.AppModuleWorker)),
						task.Requirements.GetModule(string(kernel.AppModuleWorker)),
						1,
					)
					if err != nil {
						j.log.Println("ERROR when provisioning", string(kernel.AppModuleWorker), "for task", task.GetKey())
					} else {
						workerPod = &kernel.Pod{
							NodeKey:    j.Config.SelfNode.Key(),
							Namespace:  j.Config.SelfNode.Namespace,
							PodName:    podName,
							Addr:       j.Config.SelfNode.Address,
							Port:       nodePort,
							Allocation: task.Requirements.GetModule(string(kernel.AppModuleWorker)),
							AppKey:     task.Application.Key(),
							Container:  task.Application.GetModule(string(kernel.AppModuleWorker)),
							ModuleName: string(kernel.AppModuleWorker),
						}
						workerPod.GetKey()
						j.PodCache.SetPodForApplication(
							j.Config.SelfNode.Key(),
							task.Application,
							string(kernel.AppModuleWorker),
							task.Requirements.GetModule(string(kernel.AppModuleWorker)),
							workerPod,
							true,
						)
					}
				}
				if workerPod == nil {
					// reject if the worker pod can not be allocated
					j.log.Printf("rejecting task %v", task.GetKey())
					rejectTaskCache[task.GetKey()] = taskItem
				}
			}
			if workerPod != nil && (!task.ForceUpdateNetworkStructure || j.IsSelfNode(nodekey)) {
				// 		a. if there is a woker pod in the pod-cache, then enqueue the subtask for that pod
				// 		 	 	and there should be a switch in the task data structure
				//				to indicate whether wait for updates of existing pods:
				//				for the sake of saving network overhead
				// need to wait for all the pods of sub-nodes are decided, to enqueue the subtask into the pod
				// j.PodCache.EnqueueSubtaskForPod(nodekey, workerPod, task, kernel.AppModuleWorker)
				if j.IsCoordinator() {
					j.TaskCache.CacheTaskForSubnode(task.GetKey(), j.GetNodeInControl(nodekey), string(kernel.AppModuleWorker), taskItem, workerPod)
				}
				if _, e := readyTaskCache[task.GetKey()]; !e {
					readyTaskCache[task.GetKey()] = workerPod
				}
			} else if !j.IsSelfNode(nodekey) {
				//    b. if there is a aggregator pod in the pod-cache, then dispatch the task to that node
				//		c. Otherwise If no any other kind of pod in the pod-cache,
				// 			then dispatch the task to that node, to see what kind of pod it returns,
				//			(this is the only case that a worker node could receive the entire task,
				// 				but, to update runtime structure changes, the task still shall be dispatched
				//				to the worker nodes even though there already has a worker pod-queue for that worker node)
				// 				I. If the sub-node return a worker pod, then create an item in the pod-queue for that worker pod
				// 				II. Otherwise if it is an aggregator pod, then simply cache the aggregator pod, no queue for it
				if _, e := tasksGoingToDispatch[nodekey]; !e {
					tasksGoingToDispatch[nodekey] = []*scheduler.TaskDispatchingItem{taskItem}
				} else {
					tasksGoingToDispatch[nodekey] = append(tasksGoingToDispatch[nodekey], taskItem)
				}

				if _, e := readyTaskCache[task.GetKey()]; e {
					delete(readyTaskCache, task.GetKey())
				}
				j.TaskCache.CacheTaskForSubnode(task.GetKey(), j.GetNodeInControl(nodekey), string(kernel.AppModuleWorker), taskItem, nil)
			}
			// 		d. Then cache the task into task-cache, to wait for responses from sub-nodes
			// 				I. If any sub-node responded, the task-cache could be updated,
			//						and a pod-queue for that sub-node could finally be decided,
			//						but still it’s not the time to enqueue the sub-task
			// 				II. It is until all the sub-nodes responded, the sub-tasks won’t be enqueued,
			//						because the aggregator pod haven’t be setup, or in other words,
			//						the aggregator pod haven’t be ready to receive feedbacks from worker sub-nodes
			// 		e. When all the dispatched tasks to the sub-nodes have responded,
			//				and the results are all accepted with a worker or an aggregator pod,
			// 				I. the task could be dispatched to the reducer, including the pods on the sub-nodes
			// 				II. The worker sub-tasks could be enqueued into corresponding pod-queues
		}
	}

	// dispatch sub-tasks
	for nodekey, subTasklist := range tasksGoingToDispatch {
		j.dispatchTasks(nodekey, subTasklist)
	}
	// reject bad tasks
	if len(rejectTaskCache) > 0 {
		for _, taskItem := range rejectTaskCache {
			j.TaskCache.RejectTask(taskItem.Task.GetKey())
			j.feedbackProvisioning(NewTaskProvisioningResult(
				j.Config.SelfNode.Key(),
				taskItem.Task.GetKey(),
				string(kernel.AppModuleWorker),
				nil,
			))
		}
	}
	// acknowledge good tasks
	if !j.IsCoordinator() {
		for taskKey, pod := range readyTaskCache {
			j.feedbackProvisioning(NewTaskProvisioningResult(
				j.Config.SelfNode.Key(),
				taskKey,
				string(kernel.AppModuleWorker),
				pod,
			))
		}
	} else {
		// check task status and enqueue them the task is ready
		for taskKey := range readyTaskCache {
			j.checkTaskStatus(taskKey)
		}
	}
}
