package kernel
// if talking about "registry", it actually means kernel in the implementation

import (
	ds "uta.edu/aces/jadesdk/data_structure"
	"sync"
)

// CapabilityCache in a two layers structure: capabilityName: capabilityValue: [ NodeID ]
type CapabilityCache struct {
	cache 	map[string]map[string]capabilityCacheItem
	mutex   *sync.Mutex
}

func NewCapabilityCache() *CapabilityCache {
	return &CapabilityCache{
		cache: make(map[string]map[string]capabilityCacheItem),
		mutex: &sync.Mutex{},
	}
}

type capabilityCacheItem struct {
	nodes map[string]bool
	capability *ds.Capability
}

func (c *CapabilityCache) Set(nodeId string, capabilities []*ds.Capability) {
	if nodeId == "" || len(capabilities) == 0 {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// initialize as needed
	if len(c.cache) == 0 {
		c.cache = make(map[string]map[string]capabilityCacheItem)
	}
	// remove the node from existing cache first
	for _, subcache := range c.cache {
		for _, item := range subcache {
			if _, exists := item.nodes[nodeId]; exists {
				delete(item.nodes, nodeId)
			}
		}
	}
	// then insert the node back with new capabilities
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
			item := capabilityCacheItem{
				nodes: make(map[string]bool),
			}
			item.nodes[nodeId] = true
			subcache[value] = item
			item.capability = cap
		} else {
			item.nodes[nodeId] = true
		}
	}
}

func (c *CapabilityCache) GetAllCapabilities() []*ds.Capability {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var mergedCapabilitis = make([]*ds.Capability,0)
	for name, subcache := range c.cache {
		for value, _ := range subcache { 
			// mergedCapabilitis = append(mergedCapabilitis, item.capability)
			mergedCapabilitis = append(mergedCapabilitis, &ds.Capability{
				Name: name,
				Value: value,
			})
		}
	}
	return mergedCapabilitis
}

func (c *CapabilityCache) DeleteNode(nodeId string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, subcache := range c.cache {
		for _, item := range subcache {
			if _, exists := item.nodes[nodeId]; exists {
				delete(item.nodes, nodeId)
			}
		}
	}
}

// GetNodes node Id list for that capability
func (c *CapabilityCache) getNodes(cap *ds.Capability, nodefilter []string) []string {
	if cap == nil || cap.Name == "" {
		return nil
	}
	value := cap.Value
	if len(cap.Value) == 0 {
		value = "N/A"
	}
	if subcache, subcacheExist := c.cache[cap.Name]; subcacheExist {
		if item, itemExist := subcache[value]; itemExist {
			var nodes []string = make([]string,0)
			for node, _ := range item.nodes {
				nodes = append(nodes, node)
			}
			if nodefilter == nil {
				return nodes
			} else {
				return ds.IntersectStringArrays(nodes, nodefilter)
			}
		}
	}
	return nil
}

type capabilityWithNodes struct {
	Capability *ds.Capability `json:"capability"`
	Nodes      []string           `json:"nodes"`
}

func (c *CapabilityCache) AllCapabilitiesWithNodes() []capabilityWithNodes {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var result []capabilityWithNodes
	for name, subcache := range c.cache {
		for value, item := range subcache {
			nodes := make([]string, len(item.nodes))
			i := 0
			for nodeId := range item.nodes {
				nodes[i] = nodeId
				i++
			}
			result = append(result, capabilityWithNodes{
				// Capability: item.capability,
				Capability: &ds.Capability{
					Name: name,
					Value: value,
				},
				Nodes: nodes,
			})
		}
	}
	return result
}

func (c *CapabilityCache) SelectNodesExclusively(capabilities []*ds.Capability, nodefilter []string) []string {
	if len(capabilities) == 0 {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var nodes []string
	for _, cap := range capabilities {
		tmpnodes := c.getNodes(cap, nodefilter)
		if len(tmpnodes) == 0 {
			return nil
		}
		if nodes == nil {
			nodes = tmpnodes
		} else {
			nodes = ds.IntersectStringArrays(nodes, tmpnodes)
			if len(nodes) == 0 {
				return nil
			}
		}
	}
	return nodes
}

func (c *CapabilityCache) SelectNodesCollectively(capabilities []*ds.Capability, nodefilter []string) []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var nodes []string
	for _, cap := range capabilities {
		tmpnodes := c.getNodes(cap, nodefilter)
		if len(tmpnodes) == 0 {
			continue
		}
		if nodes == nil {
			nodes = tmpnodes
		} else {
			nodes = ds.MergeStringArrays(nodes, tmpnodes)
		}
	}
	return nodes
}
