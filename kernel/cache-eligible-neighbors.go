package kernel
// if talking about "registry", it actually means kernel in the implementation

import (
	"sync"
<<<<<<< HEAD
=======
	// "log"
>>>>>>> refactoring
	ds "uta.edu/aces/jadesdk/data_structure"
)

type EligibleNeighborCacheItem struct {
<<<<<<< HEAD
	Nodes 	map[string]*ds.Node // nodeKey: node
=======
	// Nodes 	map[string]*Node // nodeKey: node
	NodeList []*ds.Node
>>>>>>> refactoring
}

func NewEligibleNeighborCacheItem(neighborNodes []*ds.Node) *EligibleNeighborCacheItem {
	cacheItem := &EligibleNeighborCacheItem{
<<<<<<< HEAD
		Nodes: make(map[string]*ds.Node),
=======
		// Nodes: make(map[string]*Node),
>>>>>>> refactoring
	}

	// for _, node := range neighborNodes {
	// 	cacheItem.Nodes[node.Key()] = node
	// }
	cacheItem.NodeList = neighborNodes

	return cacheItem
}

func (item *EligibleNeighborCacheItem) GetNeighborNodes() []*ds.Node {
<<<<<<< HEAD
	if item.Nodes == nil || len(item.Nodes) == 0 {
		return nil
	}
	node_list := make([]*ds.Node, len(item.Nodes))
	idx := 0
	for _, node := range item.Nodes {
		node_list[idx] = node
		idx++
	}
=======
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
>>>>>>> refactoring
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

func (c *EligibleNeighborCache) StoreEligibleNeighbors(requirementKey string, neighborNodes []*ds.Node) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache[requirementKey] = NewEligibleNeighborCacheItem(neighborNodes)
}

func (c *EligibleNeighborCache) GetEligibleNeighbors(requirementKey string) []*ds.Node {
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