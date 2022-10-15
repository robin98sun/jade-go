package kernel

import (
	// "time"
)

type SubTask struct {
	TaskKey           string    `json:"taskId,omitempty"`
	Key               string    `json:"key,omitempty"`
	AppName           string    `json:"appName,omitempty"`
	ModuleName        string `json:"moduleName,omitempty"`
	Fanout            int       `json:"fanout,omitempty"`
	NodeKey           string    `json:"nodeId,omitempty"`
	PodKey            string    `json:"podId,omitempty"`
	Pod               *Pod      `json:"pod,omitempty"`
}

func (t *SubTask) GetKey() string {
	if t.Key == "" {
		t.Key = t.TaskKey + ":" + string(t.ModuleName) + ":" + t.PodKey + ":" + RandomString()
	}
	return t.Key
}

func NewSubtask(taskKey string, appName string, moduleName string, nodeKey string, podKey string, subtaskKey string) *SubTask {
	newSubtask := &SubTask{
		TaskKey:    taskKey,
		AppName:    appName,
		ModuleName: moduleName,
		NodeKey:    nodeKey,
		PodKey:     podKey,
		Fanout:     1,
	}
	if subtaskKey == "" {
		newSubtask.Key = newSubtask.GetKey()
	} else {
		newSubtask.Key = subtaskKey
	}
	return newSubtask
}
