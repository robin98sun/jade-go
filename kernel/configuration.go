package kernel

import (
	"uta.edu/aces/jadesdk"
)

// Conf configuration data structure in memory
type Conf struct {
	ISA 		 string 			   `json:"isa"` // instruction structure architecture of the host
	Version      string                `json:"version"`
	UpperNode    *Node                 `json:"upperNode"`
	SelfNode     *Node                 `json:"selfNode"`
	RegistryNode *Node                 `json:"registryNode"`
	Capabilities []*jadesdk.Capability `json:"capabilities"`
	Capacity     *Capacity             `json:"capacity"`
}

// NewConfiguration construct a new configuration instance with default values
func NewConfiguration() *Conf {
	c := &Conf{}
	c.UpperNode = NewNode()
	c.SelfNode = NewNode()
	c.RegistryNode = NewNode()
	c.Capabilities = []*jadesdk.Capability{}
	c.Capacity = NewCapacity()
	return c
}

// FindCapability search a capability by name
func (c *Conf) FindCapability(name string) (int, *jadesdk.Capability) {
	if c.Capabilities == nil || len(c.Capabilities) == 0 || name == "" {
		return -1, nil
	}
	for i, cap := range c.Capabilities {
		if cap.Name == name {
			return i, cap
		}
	}
	return -1, nil
}

// AddOrUpdateCapability add or update a capability
func (c *Conf) AddOrUpdateCapability(nc *jadesdk.Capability) *jadesdk.Capability {
	if nc == nil || nc.Name == "" {
		return nil
	}
	i, found := c.FindCapability(nc.Name)
	nc.ParseAPI()
	if found != nil {
		c.Capabilities[i] = nc
	} else {
		c.Capabilities = append(c.Capabilities, nc)
	}
	return nc
}

// DeleteCapability delete a capability
func (c *Conf) DeleteCapability(name string) *jadesdk.Capability {
	if name == "" {
		return nil
	}
	i, found := c.FindCapability(name)
	if found == nil {
		return nil
	}
	c.Capabilities[len(c.Capabilities)-1], c.Capabilities[i] = c.Capabilities[i], c.Capabilities[len(c.Capabilities)-1]
	c.Capabilities = c.Capabilities[:len(c.Capabilities)-1]
	return found
}
