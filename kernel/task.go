package kernel

type TaskQueuingMechanism string

const (
	TaskQueuingFIFO TaskQueuingMechanism = "fifo"
	TaskQueuingDDL                       = "ddl"
)

type Task struct {
	Application                 *Application         `json:"application,omitempty"`
	Requirements                *Requirements        `json:"requirements,omitempty"`
	Key                         string               `json:"id,omitempty"`
	SubtaskKey                  string               `json:"subtaskId,omitempty"`
	PodKey                      string               `json:"podId,omitempty"`
	Subtasks                    map[string]*SubTask  `json:"subtasks,omitempty"`
	MasterNode                  *Node                `json:"masterNode,omitempty"`
	ForceUpdateNetworkStructure bool                 `json:"forceUpdateNetworkStructure,omitempty"`
	QueuingMechanism            TaskQueuingMechanism `json:"queuingMechanism,omitempty"`
}

func (t *Task) CopyForSubtask() *Task {
	newTask := &Task{
		Application:  t.Application,
		Requirements: t.Requirements,
		Key:          t.Key,
	}
	return newTask
}

func (t *Task) GetKey() string {
	if t.Key == "" {
		t.Key = t.Application.Key() + ":" + RandomString()
	}
	return t.Key
}

func (t *Task) NewSubtask(module string, nodeKey string, podKey string) *SubTask {
	if t == nil {
		return nil
	}
	if t.Subtasks == nil {
		t.Subtasks = make(map[string]*SubTask)
	}
	nst := NewSubtask(t.GetKey(), t.Application.Name, module, nodeKey, podKey)
	t.Subtasks[nst.GetKey()] = nst
	return nst
}

func (t *Task) GetSubtask(subtaskKey string) *SubTask {
	if t == nil || t.Subtasks == nil {
		return nil
	}
	if st, exists := t.Subtasks[subtaskKey]; exists {
		return st
	}
	return nil
}

func (t *Task) DeleteSubtask(subtaskKey string) *SubTask {
	if t == nil || t.Subtasks == nil {
		return nil
	}
	if st, exists := t.Subtasks[subtaskKey]; exists {
		delete(t.Subtasks, subtaskKey)
		return st
	}
	return nil
}

func (t *Task) Valid() bool {
	if !t.Application.valid() {
		return false
	}
	if !t.Requirements.valid() {
		return false
	}

	return true
}
