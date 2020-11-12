package jadelet

import (
	"aces/jade-go/kernel"
	"aces/jade-go/scheduler"
	"time"
)

func (j *JADE) routimeForPodQueues() {
	for {
		time.Sleep(time.Duration(1) * time.Millisecond)
		if j.PodCache == nil || len(j.PodCache.QueuingPods) == 0 {
			continue
		}
		for _, pod := range j.PodCache.QueuingPods {
			if j.PodCache.IsPodIdle(pod) {
				j.dispatchSubtask(pod)
				time.Sleep(time.Duration(1) * time.Millisecond)
			}
		}
	}
}

func (j *JADE) dispatchSubtask(pod *kernel.Pod) {
	if !j.PodCache.IsPodIdle(pod) {
		return
	}
	j.PodCache.SetPodBusy(pod)
	queue := j.PodCache.GetPodQueue(pod)
	req := queue.Dequeue()
	if req == nil {
		return
	}
	go j.HTTPCommunicate(
		"dispatch tasks", "POST", "/"+string(kernel.AppModuleWorker),
		pod.GetNodeRepresentation(j.Config.SelfNode.Protocol),
		req,
		0, 10,
	)
}

func (j *JADE) checkTaskStatus(taskKey string) {
	status := j.TaskCache.CheckTask(taskKey)
	j.log.Printf("the task{%v} is in status {%v}", taskKey, status)
	if status == scheduler.TaskStatusAccepted {
		// dispatching the task
		task := j.TaskCache.GetTask(taskKey)
		// 1. dispatch the task to the aggregator,
		//    to inform the aggregator which workers it has to wait for responses
		//   a. collect the workers
		workerSubtasks := j.TaskCache.GetSubtasks(taskKey, string(kernel.AppModuleWorker))
		if len(workerSubtasks) > 0 {
			msg := NewAggregatorEnqueuingMessage(task, workerSubtasks, j.Config.SelfNode.Protocol)
			aggregatorSubtasks := j.TaskCache.GetSubtasks(taskKey, string(kernel.AppModuleAggregator))
			if len(aggregatorSubtasks) > 0 {
				for _, aggregator := range aggregatorSubtasks {
					j.log.Println("dispatching aggregator tasks to pod", aggregator.Subtask.Pod.GetKey())
					j.HTTPCommunicate(
						"dispatch tasks", "PUT", "/$jade$/enqueueAggregativeTask",
						aggregator.Subtask.Pod.GetNodeRepresentation(j.Config.SelfNode.Protocol),
						msg,
						0, 10,
					)
				}
				// 2. dispatch the subtask to each worker,
				//    together with the aggregator's address
				for _, worker := range workerSubtasks {
					req := NewAggregativeWorkerTask(task.Task, worker, j.Config.SelfNode.Protocol)
					queue := j.PodCache.GetPodQueue(worker.Subtask.Pod)
					if queue == nil {
						continue
					}
					queue.Enqueue(req, task.Task.QueuingMechanism, 10)
				}
			}
		}
	}
}

type AggregatorEnqueuingMessage struct {
	TaskKey  string           `json:"taskId,omitempty"`
	Subtasks []string         `json:"subtasks,omitempty"`
	ReportTo []*InterfaceSpec `json:"reportTo,omitempty"`
}

func NewAggregatorEnqueuingMessage(task *scheduler.TaskDispatchingItem, subtasks []*scheduler.SubtaskOnNode, protocol string) *AggregatorEnqueuingMessage {
	inst := &AggregatorEnqueuingMessage{
		TaskKey:  task.Task.GetKey(),
		Subtasks: []string{},
		ReportTo: []*InterfaceSpec{
			&InterfaceSpec{
				Node: &NodeSpec{
					Addr:     task.ReportTo.Pod.Addr,
					Port:     task.ReportTo.Pod.Port,
					Protocol: protocol,
				},
				ModuleName: string(kernel.AppModuleAggregator),
			},
		},
	}
	for _, item := range subtasks {
		inst.Subtasks = append(inst.Subtasks, item.Subtask.GetKey())
	}

	return inst
}

func NewAggregativeWorkerTask(task *kernel.Task, worker *scheduler.SubtaskOnNode, protocol string) *Request {
	req := &Request{
		Task: &TaskSpec{
			ModuleName: kernel.AppModuleWorker,
			TaskID:     task.GetKey(),
			SubtaskID:  worker.Subtask.GetKey(),
		},
		To: []*InterfaceSpec{
			&InterfaceSpec{
				Node: &NodeSpec{
					Addr:     worker.Subtask.Pod.Addr,
					Port:     worker.Subtask.Pod.Port,
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
