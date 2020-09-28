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

type CapacityCache struct {
	cache map[string]capacityCacheItem
}

type capacityCacheItem struct {
	node              Node
	maximumCapacity   *Capacity
	remainingCapacity *Capacity
	reservedCapacity  *Capacity
}

func (c *CapacityCache) Set(nodeId string, maxcap *Capacity, remcap *Capacity) {
	if nodeId == "" || (maxcap == nil && remcap == nil) {
		return
	}
	if c.cache == nil {
		c.cache = make(map[string]capacityCacheItem)
	}

	if item, exists := c.cache[nodeId]; exists {
		if maxcap != nil {
			item.maximumCapacity = maxcap.Copy()
		}
		if remcap != nil {
			item.remainingCapacity = remcap.Copy()
		}
		c.cache[nodeId] = item
	} else {
		item = capacityCacheItem{}
		if maxcap != nil {
			item.maximumCapacity = maxcap.Copy()
		}
		if remcap != nil {
			item.remainingCapacity = remcap.Copy()
		}
		c.cache[nodeId] = item
	}
}

func (c *CapacityCache) GetMaximumCapacity(nodeId string) *Capacity {
	if nodeId == "" {
		return nil
	}
	if item, exists := c.cache[nodeId]; exists {
		if item.maximumCapacity != nil {
			return item.maximumCapacity
		}
	}
	return nil
}

func (c *CapacityCache) GetRemainingCapacity(nodeId string) *Capacity {
	if nodeId == "" {
		return nil
	}
	if item, exists := c.cache[nodeId]; exists {
		if item.remainingCapacity != nil {
			return item.remainingCapacity
		}
	}
	return nil
}

// SelectAvailableNodes select available nodes from all cache subnodes
func (c *CapacityCache) SelectAvailableNodes(cap *Capacity) []string {
	if cap == nil || c.cache == nil {
		return nil
	}
	var result []string
	for nodeID, item := range c.cache {
		if item.remainingCapacity == nil {
			continue
		}
		rc := item.remainingCapacity
		if rc.GE(cap) {
			result = append(result, nodeID)
		}
	}
	return result
}

// FilterAvailableNodes filter available nodes out of capable nodes
func (c *CapacityCache) FilterAvailableNodes(capableNodes []string, au *AllocationUnit) []string {
	if au == nil || c.cache == nil || len(capableNodes) == 0 {
		return nil
	}
	var result []string
	for _, nodeID := range capableNodes {
		if item, exists := c.cache[nodeID]; exists {
			if item.remainingCapacity == nil {
				continue
			}
			rc := item.remainingCapacity
			mc := item.maximumCapacity
			if rc.GE(au.MinimumCapacity) && mc.GE(au.MaximumCapacity) {
				result = append(result, nodeID)
			}
		}
	}
	return result
}
