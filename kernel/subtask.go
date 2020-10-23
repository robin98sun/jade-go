package kernel

import (
// "math/rand"
// "strconv"
// "time"
)

type SubTask struct {
	TaskKey string `json:"taskId,omitempty"`
	Key     string `json:"key,omitempty"`
	Module  string `json:"module,omitempty`
}

func (t *SubTask) GetKey() string {
	if t.Key == "" {
		t.Key = t.TaskKey + ":" + RandomString()
	}
	return t.Key
}

func NewSubtask(taskKey string, module string) *SubTask {
	newSubtask := &SubTask{
		TaskKey: taskKey,
		Module:  module,
	}
	newSubtask.Key = newSubtask.GetKey()
	return newSubtask
}
