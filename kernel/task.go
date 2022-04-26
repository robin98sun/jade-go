package kernel

type TaskQueuingMechanism string

const (
	TaskQueuingFIFO TaskQueuingMechanism = "fifo"
	TaskQueuingDDL                       = "ddl"
	TaskQueuingPRQ						 = "prq"
	TaskQueuingClass					 = "class"
)

type Task struct {
	Application                 *Application         `json:"application,omitempty"`
	Requirements                *Requirements        `json:"requirements,omitempty"`
	Key                         string               `json:"id,omitempty"`
	SubtaskKey                  string               `json:"subtaskId,omitempty"`
	PodKey                      string               `json:"podId,omitempty"`
	Subtasks                    map[string]*SubTask  `json:"subtasks,omitempty"`
	NeighborSubtasks            map[string]*SubTask  `json:"externalSubtasks,omitempty"`
	MasterNode                  *Node                `json:"masterNode,omitempty"`
	QueuingMechanism            TaskQueuingMechanism `json:"queuingMechanism,omitempty"`
	JobKey                      string               `json:"jobId,omitempty"`
}

func (t *Task) CopyForSubtask() *Task {
	newTask := &Task{
		Application:  t.Application,
		Requirements: t.Requirements,
		Key:          t.Key,
		QueuingMechanism: t.QueuingMechanism,
		JobKey: t.JobKey,
		// Subtasks: t.Subtasks,
		// NeighborSubtasks: t.NeighborSubtasks,
	}
	return newTask
}

func (t *Task) GetKey() string {
	if t.Key == "" {
		t.Key = t.Application.Key() + ":" + RandomString()
	}
	return t.Key
}

func (t *Task) CreateSubtask(module string, nodeKey string, podKey string, subtaskKey string, internal bool) *SubTask {
	if t == nil {
		return nil
	}
	nst := NewSubtask(t.GetKey(), t.Application.Name, module, nodeKey, podKey, subtaskKey)
	if internal {
		if t.Subtasks == nil {
			t.Subtasks = make(map[string]*SubTask)
		}
		t.Subtasks[nst.GetKey()] = nst
	} else {
		if t.NeighborSubtasks == nil {
			t.NeighborSubtasks = make(map[string]*SubTask)
		}
		t.NeighborSubtasks[nst.GetKey()] = nst
	}
	
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
