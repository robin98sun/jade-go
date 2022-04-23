package kernel
// if talking about "registry", it actually means kernel in the implementation

import (
	// "uta.edu/aces/jade-go/kernel"
	// "sync"
	// "log"
)

type EligibleNeighborCacheItem struct {
	Nodes 	map[string]*Node // nodeKey: node
}

func NewEligibleNeighborCacheItem(neighborNodes []*Node) *EligibleNeighborCacheItem {
	cacheItem := &EligibleNeighborCacheItem{
		Nodes: make(map[string]*Node),
	}

	for _, node := range neighborNodes {
		cacheItem.Nodes[node.Key()] = node
	}

	return cacheItem
}

func (item *EligibleNeighborCacheItem) GetNeighborNodes() []*Node {
	if item.Nodes == nil || len(item.Nodes) == 0 {
		return nil
	}
	node_list := make([]*Node, len(item.Nodes))
	idx := 0
	for _, node := range item.Nodes {
		node_list[idx] = node
		idx++
	}
	return node_list
}


type EligibleNeighborCache struct {
	cache map[string]*EligibleNeighborCacheItem // taskKey: nodes
}

func NewEligibleNeighborCache() *EligibleNeighborCache {
	return &EligibleNeighborCache{
		cache: make(map[string]*EligibleNeighborCacheItem),
	}
}

func (c *EligibleNeighborCache) StoreEligibleNeighbors(taskKey string, neighborNodes []*Node) {
	if c.cache == nil {
		c.cache = make(map[string]*EligibleNeighborCacheItem)
	}

	c.cache[taskKey] = NewEligibleNeighborCacheItem(neighborNodes)
}

func (c *EligibleNeighborCache) GetEligibleNeighbors(taskKey string) []*Node {
	if c.cache == nil || len(c.cache) == 0 {
		return nil
	}

	if item, e := c.cache[taskKey]; !e {
		return nil
	} else {
		return item.GetNeighborNodes()
	}
}