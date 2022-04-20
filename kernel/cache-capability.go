package kernel

import (
	"uta.edu/aces/jadesdk"
	"sync"
	"log"
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
	capability *jadesdk.Capability
}

func (c *CapabilityCache) Set(nodeId string, capabilities []*jadesdk.Capability) {
	log.Printf("setting capability cache for node %v with %v capabilities", nodeId, len(capabilities))
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
		log.Printf("  capability name: %v, value: %v", cap.Name, cap.Value)

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
			item.nodes[nodeId] = true
			subcache[value] = item
			item.capability = cap
		} else {
			item.nodes[nodeId] = true
		}
	}
	log.Printf("after setting capability cache lenght: %v",len(c.cache))
}

func (c *CapabilityCache) GetAllCapabilities() []*jadesdk.Capability {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	log.Printf("getting all capabilities from all subnodes")
	var mergedCapabilitis = make([]*jadesdk.Capability,0)
	for _, subcache := range c.cache {
		for _, item := range subcache { 
			mergedCapabilitis = append(mergedCapabilitis, item.capability)
		}
	}
	log.Printf("got %v capabilities from all subnodes", len(mergedCapabilitis))
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
func (c *CapabilityCache) getNodes(cap *jadesdk.Capability, nodefilter []string) []string {
	if cap == nil || cap.Name == "" {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
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
				return IntersectStringArrays(nodes, nodefilter)
			}
		}
	}
	return nil
}

type capabilityWithNodes struct {
	Capability *jadesdk.Capability `json:"capability"`
	Nodes      []string           `json:"nodes"`
}

func (c *CapabilityCache) AllCapabilitiesWithNodes() []capabilityWithNodes {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	log.Println("collecting all capabilities with nodes")
	var result []capabilityWithNodes
	for _, subcache := range c.cache {
		for _, item := range subcache {
			nodes := make([]string, len(item.nodes))
			i := 0
			for nodeId := range item.nodes {
				nodes[i] = nodeId
			}
			result = append(result, capabilityWithNodes{
				Capability: item.capability,
				Nodes: nodes,
			})
		}
	}
	log.Printf("collected %v capabilities with nodes, the capability cache length: %v", len(result), len(c.cache))
	return result
}

func (c *CapabilityCache) SelectNodesExclusively(capabilities []*jadesdk.Capability, nodefilter []string) []string {
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
			nodes = IntersectStringArrays(nodes, tmpnodes)
			if len(nodes) == 0 {
				return nil
			}
		}
	}
	return nodes
}

func (c *CapabilityCache) SelectNodesCollectively(capabilities []*jadesdk.Capability, nodefilter []string) []string {
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
			nodes = MergeStringArrays(nodes, tmpnodes)
		}
	}
	return nodes
}
