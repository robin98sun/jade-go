package kernel

import (
	"uta.edu/aces/jadesdk"
)

// CapabilityCache in a two layers structure: capabilityName: capabilityValue: [ NodeID ]
type CapabilityCache struct {
	cache map[string]map[string]capabilityCacheItem
}

type capabilityCacheItem struct {
	nodes []string
}

func (c *CapabilityCache) Set(nodeId string, capabilities []*jadesdk.Capability) {
	if nodeId == "" || len(capabilities) == 0 {
		return
	}
	// initialize as needed
	if len(c.cache) == 0 {
		c.cache = make(map[string]map[string]capabilityCacheItem)
	}
	// iterate the capabilities
	for _, cap := range capabilities {
		if cap.Name == "" {
			continue
		}
		// get the layer 2 subcache
		var subcache map[string]capabilityCacheItem
		var subcacheExist bool
		if subcache, subcacheExist = c.cache[cap.Name]; !subcacheExist {
			// initialize subcache when it's needed
			subcache = make(map[string]capabilityCacheItem)
			c.cache[cap.Name] = subcache
		}
		// set the key for subcache
		value := cap.Value
		if value == "" {
			value = "N/A"
		}
		// value for subcache is an item which wraps an array of nodeid
		if item, itemExist := subcache[value]; !itemExist {
			item := capabilityCacheItem{}
			item.nodes = append(item.nodes, nodeId)
			subcache[value] = item
		} else {
			item.nodes = append(item.nodes, nodeId)
			subcache[value] = item
		}
	}
}

// GetNodes node Id list for that capability
func (c *CapabilityCache) getNodes(cap *jadesdk.Capability, nodefilter []string) []string {
	if cap == nil || cap.Name == "" {
		return nil
	}
	value := cap.Value
	if len(cap.Value) == 0 {
		value = "N/A"
	}
	if subcache, subcacheExist := c.cache[cap.Name]; subcacheExist {
		if item, itemExist := subcache[value]; itemExist {
			nodes := item.nodes
			if nodefilter == nil {
				return nodes
			} else {
				return IntersectStringArrays(nodes, nodefilter)
			}
		}
	}
	return nil
}

type capabilityWithNodes struct {
	Capability jadesdk.Capability `json:"capability"`
	Nodes      []string           `json:"nodes"`
}

func (c *CapabilityCache) AllCapabilitiesWithNodes() []capabilityWithNodes {
	var result []capabilityWithNodes
	for capName, subcache := range c.cache {
		for capValue, item := range subcache {
			result = append(result, capabilityWithNodes{
				Capability: jadesdk.Capability{
					Name:  capName,
					Value: capValue,
				},
				Nodes: item.nodes,
			})
		}
	}
	return result
}

func (c *CapabilityCache) SelectNodesExclusively(capabilities []*jadesdk.Capability, nodefilter []string) []string {
	if len(capabilities) == 0 {
		return nil
	}
	var nodes []string
	for _, cap := range capabilities {
		tmpnodes := c.getNodes(cap, nodefilter)
		if len(tmpnodes) == 0 {
			return nil
		}
		if nodes == nil {
			nodes = tmpnodes
		} else {
			nodes = IntersectStringArrays(nodes, tmpnodes)
			if len(nodes) == 0 {
				return nil
			}
		}
	}
	return nodes
}

func (c *CapabilityCache) SelectNodesCollectively(capabilities []*jadesdk.Capability, nodefilter []string) []string {
	var nodes []string
	for _, cap := range capabilities {
		tmpnodes := c.getNodes(cap, nodefilter)
		if len(tmpnodes) == 0 {
			continue
		}
		if nodes == nil {
			nodes = tmpnodes
		} else {
			nodes = MergeStringArrays(nodes, tmpnodes)
		}
	}
	return nodes
}
