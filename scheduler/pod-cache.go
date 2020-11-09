package scheduler

import (
	"aces/jade-go/kernel"
)

type PodCache struct {
	Nodes map[string]*PodCacheNodeItem // nodekey: cacheItem
}

func NewPodCache() *PodCache {
	inst := &PodCache{
		Nodes: make(map[string]*PodCacheNodeItem),
	}
	return inst
}

type PodCacheNodeItem struct {
	Cache map[string]*PodCacheItem // appName+ModuleName: pod instance
}

func NewPodCacheNodeItem() *PodCacheNodeItem {
	inst := &PodCacheNodeItem{
		Cache: make(map[string]*PodCacheItem),
	}
	return inst
}

type PodCacheItem struct {
	Application *kernel.Application
	Queue       *PodQueue
	ModuleName  string
	Pod         *kernel.Pod
}

func NewPodCacheItem(app *kernel.Application, moduleName string) *PodCacheItem {
	inst := &PodCacheItem{
		Application: app,
		ModuleName:  moduleName,
		Queue:       NewPodQueue(),
		Pod:         &kernel.Pod{},
	}
	return inst
}

func (p *PodCache) GetPodForApplication(nodeKey string, app *kernel.Application, moduleName string, alloc *kernel.AllocationUnit) *kernel.Pod {
	if p == nil {
		return nil
	}
	if _, e := p.Nodes[nodeKey]; !e {
		return nil
	}

	return nil
}

func (p *PodCache) EnqueueSubtaskForPod(nodekey string, pod *kernel.Pod, task *kernel.Task, moduleName string) {

}
