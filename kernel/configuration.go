package kernel

import (
	"uta.edu/aces/jadesdk"
)


type JadeletOptions struct {
	DebugLog bool `json:"debugLog,omitempty"`
	PerfLog  bool `json:"perfLog,omitempty"`
	OpLog    bool `json:"opLog,omitempty"`
}

func NewJadeletOptions() *JadeletOptions {
	return &JadeletOptions{
		DebugLog: false,
		PerfLog:  false,
	}
}

// Conf configuration data structure in memory
type Conf struct {
	ISA 		 string 			   `json:"isa"` // instruction structure architecture of the host
	Version      string                `json:"version"`
	UpperNode    *Node                 `json:"upperNode"`
	SelfNode     *Node                 `json:"selfNode"`
	RegistryNode *Node                 `json:"registryNode"`
	Capabilities map[string][]*jadesdk.Capability `json:"capabilities"`
	Capacity     *Capacity             `json:"capacity"`
	Options      *JadeletOptions 	   `json:"options,omitempty"`
}

// NewConfiguration construct a new configuration instance with default values
func NewConfiguration() *Conf {
	c := &Conf{}
	c.UpperNode = NewNode()
	c.SelfNode = NewNode()
	c.RegistryNode = NewNode()
	c.Capabilities = make(map[string][]*jadesdk.Capability)
	c.Capacity = NewCapacity()
	c.Options = NewJadeletOptions()
	return c
}

func (c *Conf) GetAllCapabilities() []*jadesdk.Capability {
	all_capabilities := []*jadesdk.Capability{}
	for _, list := range c.Capabilities {
		all_capabilities = append(all_capabilities, list...)
	}
	return all_capabilities
}

// FindCapability search a capability by name
func (c *Conf) FindCapability(capability_type string, name string) (string, int, *jadesdk.Capability) {
	if c.Capabilities == nil || len(c.Capabilities) == 0 || name == "" {
		return "", -1, nil
	}
	for t, list := range c.Capabilities {
		if t != capability_type {continue}

		for i, cap := range list {
			if cap.Name == name {
				return t, i, cap
			}
		}
	}
	
	return "", -1, nil
}

// AddOrUpdateCapability add or update a capability
func (c *Conf) AddOrUpdateCapability(capability_type string, nc *jadesdk.Capability) *jadesdk.Capability {
	if nc == nil || nc.Name == "" {
		return nil
	}
	cap_type, i, found := c.FindCapability(capability_type, nc.Name)
	nc.ParseAPI()
	if found != nil {
		c.Capabilities[cap_type][i] = nc
	} else {
		if _, e := c.Capabilities[capability_type]; !e {
			c.Capabilities[capability_type] = []*jadesdk.Capability{}
		}
		c.Capabilities[capability_type] = append(c.Capabilities[capability_type], nc)
	}
	return nc
}

// DeleteCapability delete a capability
func (c *Conf) DeleteCapability(capability_type string, name string) *jadesdk.Capability {
	if name == "" {
		return nil
	}
	t, i, found := c.FindCapability(capability_type, name)
	if found == nil {
		return nil
	}
	c.Capabilities[t][len(c.Capabilities)-1], c.Capabilities[t][i] = c.Capabilities[t][i], c.Capabilities[t][len(c.Capabilities)-1]
	c.Capabilities[t] = c.Capabilities[t][:len(c.Capabilities)-1]
	return found
}
