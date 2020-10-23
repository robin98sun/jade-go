package kernel

import (
// "math/rand"
// "strconv"
// "time"
)

type SubTask struct {
	TaskID string `json:"taskId,omitempty"`
	Key    string `json:"key,omitempty"`
	Module string `json:"module,omitempty`
}

func (t *SubTask) GetKey() string {
	if t.Key == "" {
		t.Key = t.TaskID + ":" + RandomString()
	}
	return t.Key
}
