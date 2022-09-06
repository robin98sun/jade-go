package kernel

import (
	"sync"
	ds "uta.edu/aces/jadesdk/data_structure"
)

type CapacityStatus struct {
	MaximumCapacity   *ds.Capacity
	RemainingCapacity *ds.Capacity
	ReservedCapacity  *ds.Capacity
}

type CapacityCache struct {
	cache map[string]capacityCacheItem
	mutex *sync.Mutex
}

type capacityCacheItem struct {
	node   ds.Node
	status *CapacityStatus
}

func NewCapacityCache() *CapacityCache {
	return &CapacityCache{
		cache: make(map[string]capacityCacheItem),
		mutex: &sync.Mutex{},
	}
}

func (c *CapacityCache) Set(nodeId string, maxcap *ds.Capacity, remcap *ds.Capacity) {
	if nodeId == "" || (maxcap == nil && remcap == nil) {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.cache == nil {
		c.cache = make(map[string]capacityCacheItem)
	}

	if item, exists := c.cache[nodeId]; exists {
		if item.status == nil {
			item.status = &CapacityStatus{}
		}
		if maxcap != nil {
			item.status.MaximumCapacity = maxcap.Copy()
		}
		if remcap != nil {
			item.status.RemainingCapacity = remcap.Copy()
		}
		c.cache[nodeId] = item
	} else {
		item = capacityCacheItem{
			status: &CapacityStatus{},
		}
		if maxcap != nil {
			item.status.MaximumCapacity = maxcap.Copy()
		}
		if remcap != nil {
			item.status.RemainingCapacity = remcap.Copy()
		}
		c.cache[nodeId] = item
	}
}

func (c *CapacityCache) GetMaximumCapacity(nodeId string) *ds.Capacity {
	if nodeId == "" {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if item, exists := c.cache[nodeId]; exists {
		if item.status != nil && item.status.MaximumCapacity != nil {
			return item.status.MaximumCapacity
		}
	}
	return nil
}

func (c *CapacityCache) GetRemainingCapacity(nodeId string) *ds.Capacity {
	if nodeId == "" {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if item, exists := c.cache[nodeId]; exists {
		if item.status != nil && item.status.RemainingCapacity != nil {
			return item.status.RemainingCapacity
		}
	}
	return nil
}

// SelectAvailableNodes select available nodes from all cache subnodes
func (c *CapacityCache) SelectAvailableNodes(cap *ds.Capacity) []string {
	if cap == nil || c.cache == nil {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var result []string
	for nodeID, item := range c.cache {
		if item.status == nil || item.status.RemainingCapacity == nil {
			continue
		}
		rc := item.status.RemainingCapacity
		if rc.GE(cap) {
			result = append(result, nodeID)
		}
	}
	return result
}

// FilterAvailableNodes filter available nodes out of capable nodes
func (c *CapacityCache) FilterAvailableNodes(capableNodes []string, au *ds.AllocationUnit) []string {
	if au == nil || c.cache == nil || len(capableNodes) == 0 {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var result []string
	for _, nodeID := range capableNodes {
		if item, exists := c.cache[nodeID]; exists {
			if item.status == nil || item.status.RemainingCapacity == nil {
				continue
			}
			rc := item.status.RemainingCapacity
			mc := item.status.MaximumCapacity
			if rc.GE(au.MinimumCapacity) && mc.GE(au.MaximumCapacity) {
				result = append(result, nodeID)
			}
		}
	}
	return result
}
