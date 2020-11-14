package jadelet

import (
	"time"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler"
)

func (j *JADE) routimeForPodQueues(interval int) {
	for {
		time.Sleep(time.Duration(interval) * time.Millisecond)
		if j.PodCache == nil || len(j.PodCache.QueuingPods) == 0 {
			continue
		}
		for _, pod := range j.PodCache.QueuingPods {
			if j.PodCache.IsPodIdle(pod) {
				j.dispatchSubtask(pod)
				time.Sleep(time.Duration(interval) * time.Millisecond)
			}
		}
	}
}

func (j *JADE) dispatchSubtask(pod *kernel.Pod) {
	if !j.PodCache.IsPodIdle(pod) {
		j.log.Printf("ERROR when dispatching subtask to pod[%v]: the pod is busy", pod.GetKey())
		return
	}
	j.PodCache.SetPodBusy(pod)
	queue := j.PodCache.GetPodQueue(pod)
	req := queue.Dequeue()
	if req == nil {
		j.PodCache.SetPodIdle(pod)
		return
	}
	j.log.Printf("dispatching subtask "+pod.ModuleName+" to pod{%v [%v:%v]}: %v", pod.GetKey(), pod.Addr, pod.Port, req)
	go j.HTTPCommunicate(
		"dispatch subtask "+string(kernel.AppModuleWorker), "POST", "/"+string(kernel.AppModuleWorker),
		pod.GetNodeRepresentation(j.Config.SelfNode.Protocol),
		req,
		0, 10,
	)
}

func (j *JADE) checkTaskStatus(taskKey string) {
	// j.Lock()
	// defer j.Unlock()
	if j.TaskCache.CheckTask(taskKey, scheduler.TaskStatusAccepted, j.log.Printf) {
		j.log.Printf("the task{%v} is accepted", taskKey)
		j.TaskCache.SetTaskStatus(taskKey, scheduler.TaskStatusRunning)
		// dispatching the task
		taskItem := j.TaskCache.GetTask(taskKey)
		task := taskItem.Task
		// 1. dispatch the task to the aggregator,
		//    to inform the aggregator which workers it has to wait for responses
		//   a. collect the workers
		workerSubtasks := j.TaskCache.GetSubtasks(taskKey, string(kernel.AppModuleWorker))
		if len(workerSubtasks) > 0 {
			aggregatorSubtasks := j.TaskCache.GetSubtasks(taskKey, string(kernel.AppModuleAggregator))
			if len(aggregatorSubtasks) > 0 {
				for _, aggregator := range aggregatorSubtasks {
					msg := NewAggregatorEnqueuingMessage(taskItem, workerSubtasks, j.Config.SelfNode.Protocol)
					msg.SubtaskKey = aggregator.Subtask.GetKey()
					j.log.Println("dispatching aggregator tasks to pod", aggregator.Subtask.Pod.GetKey())
					j.HTTPCommunicate(
						"dispatch subtask "+string(kernel.AppModuleAggregator), "PUT", "/$jade$/enqueueAggregativeTask",
						aggregator.Subtask.Pod.GetNodeRepresentation(j.Config.SelfNode.Protocol),
						msg,
						0, 10,
					)
				}
				// 2. dispatch the subtask to each worker,
				//    together with the aggregator's address
				for _, worker := range workerSubtasks {
					j.log.Printf("enqueuing subtask for pod[%v] on node[%v]", worker.Subtask.Pod.GetKey(), worker.Node.Key())
					req := NewAggregativeWorkerTask(taskItem, worker, j.Config.SelfNode.Protocol)
					queue := j.PodCache.GetPodQueue(worker.Subtask.Pod)
					if queue == nil {
						j.log.Printf("ERROR when enqueuing subtask for pod[%v]: queue does not exist", worker.Subtask.Pod.GetKey())
						continue
					}
					done := queue.Enqueue(worker.Subtask.GetKey(), req, task.QueuingMechanism, 10)
					if done {
						j.log.Printf("pod[%v] enqueued subtask[%v]", worker.Subtask.Pod.GetKey(), worker.Subtask.GetKey())
					} else {
						j.log.Printf("ERROR: failed to enqueue subtask[%v] in pod[%v]", worker.Subtask.GetKey(), worker.Subtask.Pod.GetKey())
					}
				}
			}
		}
	}
}

type AggregatorEnqueuingMessage struct {
	TaskKey    string           `json:"taskId,omitempty"`
	SubtaskKey string           `json:"subtaskId,omitempty"`
	Subtasks   []string         `json:"subtasks,omitempty"`
	ReportTo   []*InterfaceSpec `json:"reportTo,omitempty"`
}

func NewAggregatorEnqueuingMessage(task *scheduler.TaskDispatchingItem, subtasks []*scheduler.SubtaskOnNode, protocol string) *AggregatorEnqueuingMessage {
	inst := &AggregatorEnqueuingMessage{
		TaskKey:  task.Task.GetKey(),
		Subtasks: []string{},
		ReportTo: []*InterfaceSpec{},
	}
	if task.ReportTo.Pod != nil {
		inst.ReportTo = append(inst.ReportTo, &InterfaceSpec{
			Node: &NodeSpec{
				Addr:     task.ReportTo.Pod.Addr,
				Port:     task.ReportTo.Pod.Port,
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

func NewAggregativeWorkerTask(taskItem *scheduler.TaskDispatchingItem, worker *scheduler.SubtaskOnNode, protocol string) *Request {
	task := taskItem.Task
	req := &Request{
		Task: &TaskSpec{
			ModuleName: kernel.AppModuleWorker,
			TaskID:     task.GetKey(),
			SubtaskID:  worker.Subtask.GetKey(),
		},
		To: []*InterfaceSpec{
			&InterfaceSpec{
				Node: &NodeSpec{
					Addr:     taskItem.ReportTo.Pod.Addr,
					Port:     taskItem.ReportTo.Pod.Port,
					Protocol: protocol,
				},
				ModuleName: string(kernel.AppModuleAggregator),
			},
		},
		Payload: worker.Subtask.Pod.Container.Input,
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

// Request message of request
type Request struct {
	Task    *TaskSpec        `json:"task,omitempty"`
	From    *InterfaceSpec   `json:"from,omitempty"`
	To      []*InterfaceSpec `json:"to,omitempty"`
	Payload interface{}      `json:"payload,omitempty"`
}
