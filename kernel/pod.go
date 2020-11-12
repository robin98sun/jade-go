package kernel

type Pod struct {
	NodeKey    string          `json:"nodeId,omitempty"`
	Namespace  string          `json:"namespace,omitempty"`
	PodName    string          `json:"podName,omitempty"`
	Addr       string          `json:"addr,omitempty"`
	Port       int             `json:"port,omitempty"`
	Allocation *AllocationUnit `json:"allocation,omitempty"`
	AppKey     string          `json:"appId,omitempty"`
	Container  *Container      `json:"container,omitempty"`
	ModuleName string          `json:"moduleName,omitempty"`
	Key        string          `json:"id,omitempty"`
}

type AllocationUnit struct {
	MinimumCapacity *Capacity `json:"minimumCapacity,omitempty"`
	MaximumCapacity *Capacity `json:"maximumCapacity,omitempty"`
}

func (a *AllocationUnit) Valid() bool {
	return a != nil && a.MaximumCapacity != nil && a.MinimumCapacity != nil && a.MaximumCapacity.GE(a.MinimumCapacity)
}

func (p *Pod) GetKey() string {
	if p.Key == "" {
		p.Key = GenPodKey(p.AppKey, p.ModuleName, p.NodeKey)
	}
	return p.Key
}

func GenPodKey(appKey string, moduleName string, nodeKey string) string {
	return appKey + ":" + moduleName + "@" + nodeKey
}

func NewPod(appKey string, moduleName string, nodeKey string) *Pod {
	return &Pod{
		NodeKey:    nodeKey,
		AppKey:     appKey,
		ModuleName: moduleName,
	}
}

func (p *Pod) GetNodeRepresentation(protocol string) *Node {
	node := NewNode()
	node.Address = p.Addr
	node.Port = p.Port
	node.Protocol = protocol
	return node
}
