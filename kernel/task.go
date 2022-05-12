package kernel

type TaskQueuingMechanism string

const (
	TaskQueuingFIFO TaskQueuingMechanism = "fifo"
	TaskQueuingPRQ	TaskQueuingMechanism = "prq"
	TaskQueuingClass TaskQueuingMechanism = "class"
	TaskQueuingDDL  TaskQueuingMechanism = "ddl"
	TaskQueuingDDL_CDF_Block  TaskQueuingMechanism = "ddl:cdf-block"
	TaskQueuingDDL_CDF_NonBlock  TaskQueuingMechanism = "ddl:cdf-non-block"
	TaskQueuingDDL_None  TaskQueuingMechanism = "ddl:none"
)

type Task struct {
	Application                 *Application         `json:"application,omitempty"`
	Requirements                *Requirements        `json:"requirements,omitempty"`
	Key                         string               `json:"id,omitempty"`
	SubtaskKey                  string               `json:"subtaskId,omitempty"`
	PodKey                      string               `json:"podId,omitempty"`
	Subtasks                    map[string]*SubTask  `json:"subtasks,omitempty"`
	NeighborNodes               map[string]*Node  `json:"neighborNodes,omitempty"`
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
	}
	return newTask
}

func (t *Task) GetKey() string {
	if t.Key == "" {
		t.Key = t.Application.Key() + ":" + RandomString()
	}
	return t.Key
}

func (t *Task) CreateSubtask(module string, nodeKey string, podKey string, subtaskKey string) *SubTask {
	if t == nil {
		return nil
	}
	nst := NewSubtask(t.GetKey(), t.Application.Name, module, nodeKey, podKey, subtaskKey)
	if t.Subtasks == nil {
		t.Subtasks = make(map[string]*SubTask)
	}
	t.Subtasks[nst.GetKey()] = nst
	return nst
}

func (t *Task) SaveNeighborNode(neighborNode *Node) {
	if t.NeighborNodes == nil {
		t.NeighborNodes = make(map[string]*Node)
	}
	t.NeighborNodes[neighborNode.GetKey()] = neighborNode
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
