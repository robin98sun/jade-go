package kernel

type Pod struct {
	NodeID        string          `json:"nodeId,omitempty"`
	Namespace     string          `json:"namespace,omitempty"`
	PodName       string          `json:"podName,omitempty"`
	ClusterIP     string          `json:"clusterIP,omitempty"`
	PodIP         string          `json:"podIP,omitempty"`
	Allocation    *AllocationUnit `json:"allocation,omitempty"`
	ApplicationID string          `json:"applicationId,omitempty"`
	TaskID        string          `json:"taskId,omitempty"`
	Container     *Container      `json:"container,omitempty"`
	SubtaskType   string          `json:"subtaskType,omitempty"` // mapper or reducer
}

type AllocationUnit struct {
	MinimumCapacity *Capacity `json:"minimumCapacity,omitempty"`
	MaximumCapacity *Capacity `json:"maximumCapacity,omitempty"`
}

func (a *AllocationUnit) Valid() bool {
	return a != nil && a.MaximumCapacity != nil && a.MinimumCapacity != nil && a.MaximumCapacity.GE(a.MinimumCapacity)
}

func (p *Pod) Key() string {
	return p.ApplicationID + ":" + p.SubtaskType
}
