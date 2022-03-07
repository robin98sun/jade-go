package jadelet

import (
	"time"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jadesdk"
)

type DispatchItemWithAggregator struct {
	DispatchingItem *scheduler.TaskDispatchingItem
	AggregatorPod *kernel.Pod
	AggregatorSubtask *kernel.SubTask
}

type SubtasksForAggregator struct {
	TaskKey string
	AggregatorPod *kernel.Pod
	SubtaskList []string
}

// evaluateTasks evaluate tasks and return a list of accepted task IDs
func (j *JADE) evaluateAggregativeTasks(tasklist map[string]*scheduler.TaskDispatchingItem) {
	rejectTaskCache := make(map[string]*scheduler.TaskDispatchingItem) // taskKey: *TaskDispatchingItem
	ackAggregatorPods := make(map[string]*kernel.Pod) // taskKey: *kernel.Pod
	ackAggregatorSubtasks := make(map[string]string) // taskKey: subtaskKey
	// first, check or allocate itself's pod
	// 1. if the node itself is a coordinator, then allocate an aggregator pod for it
	goodTaskCache := make(map[string]*DispatchItemWithAggregator) // taskKey: *TaskDispatchingItem
	for _, taskItem := range tasklist {
		task := taskItem.Task
		j.log.Printf("evaluating aggregative task with SLO: {}", taskItem.SLO)
		if j.IsCoordinator() {
			// allocate an aggregator pod if needed
			aggregatorAllocation := task.Requirements.Allocations[string(kernel.AppModuleAggregator)]
			aggregatorPod := j.PodCache.GetPodForApplication(j.SelfNodeKey(), task.Application, string(kernel.AppModuleAggregator), aggregatorAllocation)
			if aggregatorPod == nil {
				// provision an aggregator pod
				containerSettings := task.Application.GetModule(string(kernel.AppModuleAggregator))
				containerSettings.SetISAInImage(j.Config.ISA)
				podName, nodePort, err := j.Provisioner.ProvisionTask(
					j.Kube, j.Config.SelfNode,
					j.newEnv(
						j.Config.SelfNode,
						task.Application.Name,
						task.Application.Version,
						string(kernel.AppModuleAggregator),
						task.GetKey(),
					),
					task.Application, string(kernel.AppModuleAggregator),
					containerSettings,
					task.Requirements.GetModule(string(kernel.AppModuleAggregator)),
					1,
				)
				//
				if err != nil {
					j.log.Println("ERROR when provisioning", string(kernel.AppModuleAggregator), "for task", task.GetKey())
				} else if err = j.updatePodConfigOfSelfNodePort(nodePort); err != nil {
					// Update self-node inside the pod
					j.log.Println("Error when updating pod configuration:", err)
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
				newTaskItem := taskItem.CopyForSubtask(false)
				// the reportTo is very tricky here
				// it's different for aggregator and worker module
				// please think carefully why they are different
				// that's critical of testing whether your understandings of dataflow are correct
				reportTo := taskItem.GetReportToForModule(kernel.AppModuleWorker)
				if reportTo != nil && reportTo.Node != nil && reportTo.Pod != nil {
					newTaskItem.SetReportToForModule(string(kernel.AppModuleAggregator), reportTo.Node, reportTo.Pod)
				}
				subtask := j.TaskCache.CacheTaskForSubnode(task.GetKey(), j.Config.SelfNode, string(kernel.AppModuleAggregator), newTaskItem, aggregatorPod, "", task.SubtaskKey, j.log.Printf)
				newTaskItem.SetReportToForModule(kernel.AppModuleWorker, j.Config.SelfNode, aggregatorPod)
				goodTaskCache[task.GetKey()] = &DispatchItemWithAggregator{
					DispatchingItem: newTaskItem,
					AggregatorPod: aggregatorPod,
					AggregatorSubtask: subtask,
				}
				ackAggregatorPods[task.GetKey()] = aggregatorPod
				ackAggregatorSubtasks[task.GetKey()] = subtask.GetKey()
			}
		} else {
			goodTaskCache[task.GetKey()] = &DispatchItemWithAggregator{
				DispatchingItem: taskItem,
				AggregatorPod: nil,
				AggregatorSubtask: nil,
			}
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
				"",
			))
		}
	}
	// acknowledge good tasks
	if j.HasUpperNode() && len(ackAggregatorPods) > 0 {
		for taskKey, pod := range ackAggregatorPods {
			j.log.Printf("Acknowledging good task[%v] before propagating for module[%v] of application[%v], pod key: %v", taskKey, pod.ModuleName, pod.AppKey, pod.GetKey())
			aggregatorSubtaskKey := ackAggregatorSubtasks[taskKey]
			j.feedbackProvisioning(NewTaskProvisioningResult(
				j.Config.SelfNode.Key(),
				taskKey,
				string(kernel.AppModuleAggregator),
				pod,
				aggregatorSubtaskKey,
			))
		}
	}
	
}

func (j *JADE) updatePodConfigOfSelfNodePort(nodePort int) error {
	// seconds := 15
	seconds := 60 
	j.log.Printf("waiting {%v} seconds for pod up", seconds)
	time.Sleep(time.Duration(seconds) * time.Second)
	newConf := &jadesdk.Conf{
		SelfNode: &jadesdk.Node{
			Addr:     j.Config.SelfNode.Address,
			Port:     nodePort,
			Protocol: j.Config.SelfNode.Protocol,
		},
	}
	if j.Config != nil && j.Config.Capabilities != nil && len(j.Config.Capabilities) > 0 {
		newConf.Capabilities = j.Config.Capabilities
	}
	_, _, err := j.sdk.HTTPCommunicate(
		"update configuration", j.Config.SelfNode.Protocol,
		"PUT", "/$jade$/config", newConf.SelfNode, newConf,
		0, 2000,
	)

	return err
}

func (j *JADE) downstreamPropagating(tasklist map[string]*DispatchItemWithAggregator) {
	tasksGoingToDispatch := make(map[string][]*DispatchItemWithAggregator) // nodekey: []*TaskDispatchingItem
	readyTaskCache := make(map[string]*kernel.Pod)                            // taskkey: *Pod
	rejectTaskCache := make(map[string]*scheduler.TaskDispatchingItem)        // taskKey: *TaskDispatchingItem
	for _, disptachItem := range tasklist {
		taskItem := disptachItem.DispatchingItem
		task := taskItem.Task
		reportTo := taskItem.GetReportToForModule(string(kernel.AppModuleWorker))
		j.log.Printf("evaluating task[%v], report to [%v]", task.GetKey(), reportTo)
		if reportTo == nil {
			j.log.Printf("ERROR while evaluating task[%v], no 'report to' setting", task.GetKey())
			continue
			// very weird right? 
			// please see line 72
			// that's where the reportTo is defined or setup for the original request
			// so at this point, the reportTo must not be empty
			// unless the request is illegally initiated to a worker node
		}
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
			aggregatorAllocation := task.Requirements.Allocations[string(kernel.AppModuleAggregator)]
			workerPod := j.PodCache.GetPodForApplication(nodekey, task.Application, string(kernel.AppModuleWorker), workerAllocation)
			aggregatorPod := j.PodCache.GetPodForApplication(nodekey, task.Application, string(kernel.AppModuleAggregator), aggregatorAllocation)
			// if the node itself is also a worker, then allcate a worker pod for it
			if j.IsSelfNode(nodekey) {
				j.log.Printf("[%v] is a self-node", nodekey)
				if workerPod == nil {
					// provision a worker Pod for it
					containerSettings := task.Application.GetModule(string(kernel.AppModuleWorker))
					containerSettings.SetISAInImage(j.Config.ISA)
					podName, nodePort, err := j.Provisioner.ProvisionTask(
						j.Kube, j.Config.SelfNode,
						j.newEnv(
							reportTo.Node,
							task.Application.Name,
							task.Application.Version,
							string(kernel.AppModuleWorker),
							task.GetKey(),
						),
						task.Application, string(kernel.AppModuleWorker),
						containerSettings,
						task.Requirements.GetModule(string(kernel.AppModuleWorker)),
						1,
					)

					if err != nil {
						j.log.Println("ERROR when provisioning", string(kernel.AppModuleWorker), "for task", task.GetKey())
						// Update self-node inside the pod
					} else if err = j.updatePodConfigOfSelfNodePort(nodePort); err != nil {
						j.log.Println("ERROR when updating pod nodePort", string(kernel.AppModuleWorker), "for task", task.GetKey())
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
					j.log.Printf("Caching pod[%v] on node[%v] for task[%v]", workerPod.GetKey(), nodekey, task.GetKey())
					j.TaskCache.CacheTaskForSubnode(task.GetKey(), j.GetNodeInControl(nodekey), string(kernel.AppModuleWorker), taskItem, workerPod, "", "", j.log.Printf)
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
				j.log.Printf("Found an aggregator pod or unknown type pod on node[%v]", nodekey)
				
				var subtask *kernel.SubTask
				if aggregatorPod == nil {
					j.log.Printf("Caching empty pod on node[%v] for task[%v]", nodekey, task.GetKey())
					subtask = j.TaskCache.CacheTaskForSubnode(task.GetKey(), j.GetNodeInControl(nodekey), string(kernel.AppModuleWorker), taskItem, nil, "", "", j.log.Printf)
				} else {
					j.log.Printf("Caching aggregator pod on node[%v] for task[%v]", nodekey, task.GetKey())
					subtask = j.TaskCache.CacheTaskForSubnode(task.GetKey(), j.GetNodeInControl(nodekey), string(kernel.AppModuleAggregator), taskItem, aggregatorPod, "", "", j.log.Printf)
				}
				if _, e := readyTaskCache[task.GetKey()]; e {
					delete(readyTaskCache, task.GetKey())
				}
				newDispatchItem := &DispatchItemWithAggregator {
					DispatchingItem: disptachItem.DispatchingItem.CopyForSubtask(true),
					AggregatorPod: disptachItem.AggregatorPod,
					AggregatorSubtask: disptachItem.AggregatorSubtask,
				}
				if subtask != nil {
					newDispatchItem.DispatchingItem.Task.SubtaskKey = subtask.GetKey()
				}
				if _, e := tasksGoingToDispatch[nodekey]; !e {
					tasksGoingToDispatch[nodekey] = []*DispatchItemWithAggregator{newDispatchItem}
				} else {
					tasksGoingToDispatch[nodekey] = append(tasksGoingToDispatch[nodekey], newDispatchItem)
				}
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
	nodesToDispatch := make(map[string][]*scheduler.TaskDispatchingItem)
	for nodekey, subTasklist := range tasksGoingToDispatch {
		dispatchingList := []*scheduler.TaskDispatchingItem{}
		for _, dispatchItem := range subTasklist {
			originalTaskItem := dispatchItem.DispatchingItem
			dispatchingList = append(dispatchingList, originalTaskItem)
		}
		nodesToDispatch[nodekey] = dispatchingList
	}

	j.log.Printf("Further dispatching subtasks to {%v} sub-nodes", len(nodesToDispatch))
	for nodekey, dispatchingList := range nodesToDispatch {
		j.dispatchTasks(nodekey, dispatchingList)
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
				taskItem.Task.SubtaskKey,
			))
		}
	}
	// acknowledge good tasks as a worker node
	for taskKey, pod := range readyTaskCache {
		if j.IsSelfNode(pod.NodeKey) {
			j.log.Printf("Acknowledging good task[%v] after propagating for module[%v] of application[%v], pod key: %v", taskKey, pod.ModuleName, pod.AppKey, pod.GetKey())
			j.feedbackProvisioning(NewTaskProvisioningResult(
				j.Config.SelfNode.Key(),
				taskKey,
				pod.ModuleName,
				pod,
				"",
			))
		}
	}
	if j.IsCoordinator() {
		// check task status and enqueue them the task is ready
		for taskKey := range readyTaskCache {
			j.log.Printf("check status for good task[%v] after propagating", taskKey)
			j.checkTaskStatus(taskKey)
		}
	}
}
