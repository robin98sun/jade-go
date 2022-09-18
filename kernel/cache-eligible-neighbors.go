package kernel
// if talking about "registry", it actually means kernel in the implementation

import (
	// "uta.edu/aces/jade-go/kernel"
	"sync"
	// "log"
)

type EligibleNeighborCacheItem struct {
	// Nodes 	map[string]*Node // nodeKey: node
	NodeList []*Node
}

func NewEligibleNeighborCacheItem(neighborNodes []*Node) *EligibleNeighborCacheItem {
	cacheItem := &EligibleNeighborCacheItem{
		// Nodes: make(map[string]*Node),
	}

	// for _, node := range neighborNodes {
	// 	cacheItem.Nodes[node.Key()] = node
	// }
	cacheItem.NodeList = neighborNodes

	return cacheItem
}

func (item *EligibleNeighborCacheItem) GetNeighborNodes() []*Node {
	if item.NodeList == nil || len(item.NodeList) == 0 {
		return nil
	}
	// node_list := make([]*Node, len(item.Nodes))
	// idx := 0
	// for _, node := range item.Nodes {
	// 	node_list[idx] = node
	// 	idx++
	// }
	node_list := item.NodeList
	return node_list
}


type EligibleNeighborCache struct {
	cache map[string]*EligibleNeighborCacheItem // query-key: nodes
	mutex *sync.Mutex
}

func NewEligibleNeighborCache() *EligibleNeighborCache {
	return &EligibleNeighborCache{
		cache: make(map[string]*EligibleNeighborCacheItem),
		mutex: &sync.Mutex{},
	}
}

func (c *EligibleNeighborCache) StoreEligibleNeighbors(requirementKey string, neighborNodes []*Node) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache[requirementKey] = NewEligibleNeighborCacheItem(neighborNodes)
}

func (c *EligibleNeighborCache) GetEligibleNeighbors(requirementKey string) []*Node {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.cache) == 0 {
		return nil
	}

	if item, e := c.cache[requirementKey]; !e {
		return nil
	} else {
		return item.GetNeighborNodes()
	}
}