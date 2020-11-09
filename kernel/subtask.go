package kernel

type SubTask struct {
	TaskKey    string `json:"taskId,omitempty"`
	Key        string `json:"key,omitempty"`
	ModuleName string `json:"moduleName,omitempty"`
	NodeKey    string `json:"nodeId,omitempty"`
	PodKey     string `json:"podId,omitempty"`
}

func (t *SubTask) GetKey() string {
	if t.Key == "" {
		t.Key = t.TaskKey + ":" + t.ModuleName + ":" + t.PodKey + ":" + RandomString()
	}
	return t.Key
}

func NewSubtask(taskKey string, moduleName string, nodeKey string, podKey string) *SubTask {
	newSubtask := &SubTask{
		TaskKey:    taskKey,
		ModuleName: moduleName,
		NodeKey:    nodeKey,
		PodKey:     podKey,
	}
	newSubtask.Key = newSubtask.GetKey()
	return newSubtask
}
