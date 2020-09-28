package kernel

import (
//
)

type PodCache struct {
	cache map[string]*PodCacheItem // key: applicationID + subtaskType
}

type PodCacheItem struct {
	pod *Pod
}

func (c *PodCache) Set(pod *Pod) {
	if pod == nil {
		return
	}
	if c.cache == nil {
		c.cache = make(map[string]*PodCacheItem)
	}
	key := pod.Key()
	if item, exists := c.cache[key]; exists {
		item.pod = pod
		c.cache[key] = item
	} else {
		item = &PodCacheItem{}
		item.pod = pod
		c.cache[key] = item
	}
}

func (c *PodCache) Get(key string) *Pod {
	if c.cache == nil {
		return nil
	}
	if item, exists := c.cache[key]; exists {
		return item.pod
	}
	return nil
}

func (c *PodCache) GetPodForApplication(appKey string, subtaskType string) *Pod {
	key := appKey + ":" + subtaskType
	return c.Get(key)
}
