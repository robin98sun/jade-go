package jadelet

import (
	"aces/jade-go/kernel"
)

// evaluateTasks evaluate tasks and return a list of accepted task IDs
func (j *JADE) evaluateAggregativeTasks(tasklist map[string]*kernel.Task) {
	rejectedTasks := map[string][]string{}
	acceptedTasks := map[string][]string{}

	targetNodes := make(map[string][]*kernel.Task)

	for _, task := range tasklist {
		j.log.Println("evaluating task:", task.GetKey())
		j.taskCache.Set(task, nil, nil, kernel.TaskStatusPending, nil)
		// Part 1: on the node which is running as an aggregator
		//   stage 1: check capabilitis and capacities
		//   at the very beginning, just assuming aggregator is unlimited
		//   only check for workers
		//   1. search all required sub-nodes
		//   2. check if there is already a pod/pods for this kind of task on the sub-node
		//   3. if has, pick a pod which queuing time is acceptable for the task's budget
		//   4. 	if no acceptable queuing time for all pods, then reject the task
		//   5. 	otherwise, collect the pod key for the node
		//   6. if no existing pod for the task, check if the node has sufficient capacity for the task
		//   7. 	if capacity is not sufficient, then reject the task

		//   stage 2: just generate subtasks for the aggregator to wait for
		//   1. generate subtasks for all selected sub-nodes

		//   stage 3: get the address of aggregator
		//   for the aggregator, by nature it is going to on the self-node
		//   but it might change in future design
		//   1. find out whether the aggregator's pod is there
		//   2. if not, provision the aggregator's pod first
		//      (real privisioning no matter it is or not on the self-node)
		//   3. dispatch the aggregator task to that pod, using the subtasks data generated in previous stage
		//   4. get the aggregator pod's address and port

		//   stage 4: provision worker pods on sub-nodes which does not have one yet
		//   for the workers, they are on sub-nodes by nature,
		//   but still might including self-node, otherwise, signle-node model won't work
		//   1. for each capable but not provisioned sub-node
		//   2. pretend to provision a pod for the worker: just create a pod key
		// 		  the actual provisioning of worker pod will take place in worker node
		//      it's for concept independency, considering if the system has no backup of k8s/k3s cluster mechanism
		//   3. collect the pod key for that node
		//   4. dispatch the pod-provisioning commands to these sub-nodes,
		//   5. wait for the feedbacks of provisioning from sub-nodes,
		//   6. if some sub-node failed the provisioning, then reject the task to the upper node

		//   stage 5: enqueue sub-tasks for all sub-nodes
		//   no need to wait for all provisionings, just go ahead whenever a sub-node is ready
		//   1. enqueue sub-task for that node's selected pod

		// Part 2: Dequeue a sub-task and dispatch the task to that pod
		//   1. when a pod is idle, which event is triggered when a pod reported to the master
		//   2. dequeue a sub-task for that pod
		//   3. dispatch that sub-task

		// Part 3: on the node which is running as a worker
		//   stage 1: provisioning
		// 	 1. provision a pod according to the provisioning command
		//   2. feedback success/failure of provisioning

		// prepare environments
		accept := true
		selfInTarget := false
		workerTask := task
		nodeToSubtask := map[string]string{}
		// firstly deploy aggregator
		if j.IsAggregator() {
			masterNode := task.MasterNode
			if masterNode == nil || masterNode.IsAddrEmpty() {
				if j.Config.UpperNode.IsAddrEmpty() {
					masterNode = j.Config.SelfNode
				} else {
					masterNode = j.Config.UpperNode
				}
			}
			nodeToSubtask = j.processAggregator(task, targetNodes, masterNode)
			if nodeToSubtask == nil {
				accept = false
			}
			if tasklist, exists := targetNodes[j.Config.SelfNode.Key()]; exists {
				for _, t := range tasklist {
					if t.GetKey() == task.GetKey() {
						selfInTarget = true
						workerTask = t
						break
					}
				}
			}
		}
		// then deploy worker
		// notice, the aggregator could also be a worker
		if accept && j.IsWorker() || selfInTarget {
			subtask := task.GetSubtask(nodeToSubtask[j.Config.SelfNode.Key()])
			if !selfInTarget {
				subtask = nil
			}
			masterNode := task.MasterNode
			if masterNode == nil || masterNode.IsAddrEmpty() {
				if selfInTarget || j.Config.UpperNode.IsAddrEmpty() {
					masterNode = j.Config.SelfNode
				} else {
					masterNode = j.Config.UpperNode
				}
			}
			accept = j.processWorker(workerTask, subtask, masterNode)
		}
		if accept && selfInTarget {
			j.log.Println("accepted task as a worker:", task.GetKey())
			subtask := task.GetSubtask(workerTask.SubtaskKey)
			j.taskCache.Set(task, j.Config.SelfNode, subtask, kernel.TaskStatusAccepted, nil)
		} else if accept && j.IsWorker() {
			j.log.Println("accepted task as a worker:", task.GetKey())
			acceptedTasks[task.GetKey()] = append(acceptedTasks[task.GetKey()], task.SubtaskKey)
			j.taskCache.Set(task, nil, nil, kernel.TaskStatusAccepted, nil)
		} else if !accept {
			j.log.Println("rejected task:", task.GetKey())
			rejectedTasks[task.GetKey()] = append(rejectedTasks[task.GetKey()], task.SubtaskKey)
			j.taskCache.Set(task, nil, nil, kernel.TaskStatusRejected, nil)
		}
	}

	// downstream: dispatch sub tasks
	// in `background-downstream-task-planner.go`
	if len(targetNodes) > 0 {
		for nodeID, tasks := range targetNodes {
			if nodeID != j.Config.SelfNode.Key() {
				j.dispatchTasks(nodeID, tasks)
			}
		}
	}

	// upstream: feed back acceptances
	// in `background-upstream-forwarder.go`
	result := &TaskEvalReslllkkjy7t655ult{
		Accepted: acceptedTasks,
		Rejected: rejectedTasks,
	}
	if !result.IsEmpty() {
		j.feedbackTaskAcceptances(result)
	}
}
