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

	// dispatch sub tasks
	if len(targetNodes) > 0 {
		for nodeID, tasks := range targetNodes {
			if nodeID != j.Config.SelfNode.Key() {
				j.dispatchTasks(nodeID, tasks)
			}
		}
	}

	// feed back acceptances
	result := &TaskEvalResult{
		Accepted: acceptedTasks,
		Rejected: rejectedTasks,
	}
	if !result.IsEmpty() {
		j.feedbackTaskAcceptances(result)
	}
}
