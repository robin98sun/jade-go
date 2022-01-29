package scheduler

import (
	"time"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jadesdk"
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
	PreServiceTime     time.Duration `json:"preServiceTime,omitempty"` // for aggregator it is cumulative pre-service time
	ExecutionTime      time.Duration `json:"executionTime,omitempty"` // for aggregator cumulative execution time, for worker it is the same as service time
	ServiceTime        time.Duration `json:"serviceTime,omitempty"`  // for aggregator it is from the arrival time to the overall finish time, for worker it is the same as execution time
	PostServiceTime    time.Duration `json:"postServiceTime,omitempty"`
	RequestTime        time.Duration `json:"requestTime,omitempty"`
	ForwardingTime     time.Duration `json:"forwardingTime,omitempty"`
	CommunicationTime  time.Duration `json:"communicationTime,omitempty"`
	EnqueuingOverhead  time.Duration `json:"enqueuingOverhead,omitempty"`
	AmountPreempted    int           `json:"amountPreempted,omitempty"`
	Budget             float64         `json:"budget,omitempty"`
	Priority           int           `json:"priority,omitempty"`
	RetryCountOfSending int64        `json:"retryCountOfSending,omitempty"`
	RetryCountOfReceiving int64      `json:"retryCountOfReceiving,omitempty"`
	PreDispatchingTime time.Duration `json:"preDispatchingTime,omitempty"`
	ReportProcessingTime time.Duration `json:"reportProcessingTime,omitempty"`
	MetricsEnv 		   *jadesdk.MetricsEnv `json:"metricsEnv,omitempty"`
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
