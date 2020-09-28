package kernel

import (
// "aces/jade-go/kube"
// "bytes"
// "encoding/json"
// "errors"
// "github.com/ant0ine/go-json-rest/rest"
// "log"
// "net/http"
// "time"
)

// CapabilityCache in a two layers structure: capabilityName: capabilityValue: [ NodeID ]
type CapabilityCache struct {
	cache map[string]map[string]capabilityCacheItem
}

type capabilityCacheItem struct {
	nodes []string
}

func (c *CapabilityCache) Set(nodeId string, capabilities []*Capability) {
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
func (c *CapabilityCache) getNodes(cap *Capability) []string {
	if cap == nil || cap.Name == "" {
		return nil
	}
	value := cap.Value
	if len(cap.Value) == 0 {
		value = "N/A"
	}
	if subcache, subcacheExist := c.cache[cap.Name]; subcacheExist {
		if item, itemExist := subcache[value]; itemExist {
			return item.nodes
		}
	}
	return nil
}

type capabilityWithNodes struct {
	Capability Capability `json:"capability"`
	Nodes      []string   `json:"nodes"`
}

func (c *CapabilityCache) AllCapabilitiesWithNodes() []capabilityWithNodes {
	var result []capabilityWithNodes
	for capName, subcache := range c.cache {
		for capValue, item := range subcache {
			result = append(result, capabilityWithNodes{
				Capability: Capability{
					Name:  capName,
					Value: capValue,
				},
				Nodes: item.nodes,
			})
		}
	}
	return result
}

func (c *CapabilityCache) SelectNodes(capabilities []*Capability) []string {
	if len(capabilities) == 0 {
		return nil
	}
	var nodes []string
	for _, cap := range capabilities {
		tmpnodes := c.getNodes(cap)
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
