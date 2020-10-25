package kernel

type SubTask struct {
	TaskKey string `json:"taskId,omitempty"`
	Key     string `json:"key,omitempty"`
	Module  string `json:"module,omitempty"`
	NodeKey string `json:"nodeId,omitempty"`
	PodKey  string `json:"podId,omitempty"`
}

func (t *SubTask) GetKey() string {
	if t.Key == "" {
		t.Key = t.TaskKey + ":" + t.Module + ":" + t.PodKey + ":" + RandomString()
	}
	return t.Key
}

func NewSubtask(taskKey string, module string, nodeKey string, podKey string) *SubTask {
	newSubtask := &SubTask{
		TaskKey: taskKey,
		Module:  module,
		NodeKey: nodeKey,
		PodKey:  podKey,
	}
	newSubtask.Key = newSubtask.GetKey()
	return newSubtask
}
