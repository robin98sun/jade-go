package scheduler

import (
	"time"
	"uta.edu/aces/jade-go/kernel"
)

type TaskCacheModuleItem struct {
	subtasks map[string]*TaskCacheSubtaskItem
	status   TaskStatus
}

func (i *TaskCacheModuleItem) describe() map[string]interface{} {
	if i == nil {
		return nil
	}
	subtasksDesc := map[string]interface{}{}
	if len(i.subtasks) == 0 {
		subtasksDesc = nil
	} else {
		for key, value := range i.subtasks {
			subtasksDesc[key] = value.describe()
		}
	}
	desc := map[string]interface{}{
		"subtasks": subtasksDesc,
		"status":   i.status,
	}
	return desc
}

type TaskCacheSubtaskItem struct {
	subtask            *kernel.SubTask
	status             TaskStatus
	updates            interface{}
	ArriveTimestamp    time.Time     `json:"arriveTimestemp,omitempty"`
	EnqueueTimestamp   time.Time     `json:"enqueueTimestemp,omitempty"`
	QueueLength        int64         `json:"queueLength,omitempty"`
	SendPackageSize    int           `json:"sendPackageSize,omitempty"`
	ReceivePackageSize int           `json:"receivePackageSize,omitempty"`
	DispatchTimestamp  time.Time     `json:"dispatchTimestamp,omitempty"`
	FinishTimestamp    time.Time     `json:"finishTimestamp,omitempty"`
	QueueingTime       time.Duration `json:"queueingTime,omitempty"`
	ServiceTime        time.Duration `json:"serviceTime,omitempty"`
	RequestTime        time.Duration `json:"requestTime,omitempty"`
	ForwardingTime     time.Duration `json:"forwardingTime,omitempty"`
	RTT                time.Duration `json:"RTT,omitempty"`
}

func (i *TaskCacheSubtaskItem) describe() map[string]interface{} {
	if i == nil {
		return nil
	}
	desc := map[string]interface{}{
		"subtask": i.subtask,
		"status":  i.status,
		"updates": i.updates,
	}
	return desc
}
