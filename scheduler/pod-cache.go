package scheduler

import (
	"sort"
	"sync"
	"uta.edu/aces/jade-go/kernel"
)

type PodCache struct {
	Nodes                      map[string]*PodCacheNodeItem // nodekey: cacheItem
	mutex                      *sync.Mutex
	Pods                       map[string]*kernel.Pod
	QueuingPods                map[string]*kernel.Pod
	IsBackgroundRoutineStarted bool
}

func (p *PodCache) Lock() {
	p.mutex.Lock()
}

func (p *PodCache) Unlock() {
	p.mutex.Unlock()
}

func (p *PodCache) Clear() {
	p.Lock()
	defer p.Unlock()
	for key := range p.Nodes {
		delete(p.Nodes, key)
	}

	for key := range p.Pods {
		delete(p.Pods, key)
	}

	for key := range p.QueuingPods {
		delete(p.QueuingPods, key)
	}
}

func NewPodCache() *PodCache {
	inst := &PodCache{
		Nodes: make(map[string]*PodCacheNodeItem),
		mutex: &sync.Mutex{},
	}
	return inst
}

type PodCacheNodeItem struct {
	AppModules map[string]*PodCacheAppModuleItem // appName+ModuleName: pod instance
}

type PodCacheAppModuleItem struct {
	Cache map[string]*PodCacheItem
	List  []*PodCacheItem
}

func NewPodCacheAppModuleItem() *PodCacheAppModuleItem {
	inst := &PodCacheAppModuleItem{
		Cache: make(map[string]*PodCacheItem),
		List:  []*PodCacheItem{},
	}
	return inst
}

func NewPodCacheNodeItem() *PodCacheNodeItem {
	inst := &PodCacheNodeItem{
		AppModules: make(map[string]*PodCacheAppModuleItem),
	}
	return inst
}

type PodCacheItem struct {
	Application *kernel.Application
	Queue       *PodQueue
	ModuleName  string
	Allocation  *kernel.AllocationUnit
	Pod         *kernel.Pod
	IsIdle      bool
}

func NewPodCacheItem(app *kernel.Application, moduleName string, alloc *kernel.AllocationUnit, pod *kernel.Pod) *PodCacheItem {
	inst := &PodCacheItem{
		Application: app,
		ModuleName:  moduleName,
		Queue:       NewPodQueue(),
		Allocation:  alloc,
		Pod:         pod,
		IsIdle:      true,
	}
	inst.Queue.Pod = pod
	return inst
}

func (p *PodCache) SetPodIdle(pod *kernel.Pod) {
	p.setPodIdleOrNot(pod, true)
}
func (p *PodCache) SetPodBusy(pod *kernel.Pod) {
	p.setPodIdleOrNot(pod, false)
}
func (p *PodCache) setPodIdleOrNot(pod *kernel.Pod, idle bool) {
	p.Lock()
	defer p.Unlock()
	if p == nil || len(p.Nodes) == 0 || pod == nil {
		return
	}
	if nodeItem, e := p.Nodes[pod.NodeKey]; e {
		key := p.GetKeyFromApplicationAndModule(pod.AppKey, pod.ModuleName)
		if appModuleItem, e := nodeItem.AppModules[key]; e && len(appModuleItem.List) > 0 {
			if podItem, e := appModuleItem.Cache[pod.GetKey()]; e {
				podItem.IsIdle = idle
			}
		}
	}
}
func (p *PodCache) IsPodIdle(pod *kernel.Pod) bool {
	p.Lock()
	defer p.Unlock()
	if p == nil || len(p.Nodes) == 0 || pod == nil {
		return false
	}

	if nodeItem, e := p.Nodes[pod.NodeKey]; e {
		key := p.GetKeyFromApplicationAndModule(pod.AppKey, pod.ModuleName)
		if appModuleItem, e := nodeItem.AppModules[key]; e && len(appModuleItem.List) > 0 {
			if podItem, e := appModuleItem.Cache[pod.GetKey()]; e {
				return podItem.IsIdle
			}
		}
	}
	return false
}

func (p *PodCache) GetPodForApplication(nodeKey string, app *kernel.Application, moduleName string, alloc *kernel.AllocationUnit) *kernel.Pod {
	if p == nil {
		return nil
	}
	p.Lock()
	defer p.Unlock()

	if nodeItem, e := p.Nodes[nodeKey]; e {
		key := p.GetKeyFromApplicationAndModule(app.Key(), moduleName)
		if appModuleItem, e := nodeItem.AppModules[key]; e && len(appModuleItem.List) > 0 {
			for _, podItem := range appModuleItem.List {
				if podItem.Allocation.MinimumCapacity.GE(alloc.MinimumCapacity) {
					return podItem.Pod
				}
			}
		}
	}
	return nil
}

func (p *PodCache) GetPodQueue(pod *kernel.Pod) *PodQueue {
	if pod == nil {
		return nil
	}
	p.Lock()
	defer p.Unlock()

	if nodeItem, e := p.Nodes[pod.NodeKey]; e {
		key := p.GetKeyFromApplicationAndModule(pod.AppKey, pod.ModuleName)
		if appModuleItem, e := nodeItem.AppModules[key]; e && len(appModuleItem.List) > 0 {
			if podItem, e := appModuleItem.Cache[pod.GetKey()]; e {
				return podItem.Queue
			}
		}
	}
	return nil
}

func (p *PodCache) GetKeyFromApplicationAndModule(appKey string, moduleName string) string {
	return appKey + ":" + moduleName
}

func (p *PodCache) SetPodForApplication(nodeKey string, app *kernel.Application, moduleName string, alloc *kernel.AllocationUnit, pod *kernel.Pod, enqueue bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if _, e := p.Pods[pod.GetKey()]; e {
		return
	}

	if p.Nodes == nil {
		p.Nodes = make(map[string]*PodCacheNodeItem)
	}
	if _, e := p.Nodes[nodeKey]; !e {
		p.Nodes[nodeKey] = NewPodCacheNodeItem()
	}
	nodeItem := p.Nodes[nodeKey]
	key := p.GetKeyFromApplicationAndModule(app.Key(), moduleName)

	if _, e := nodeItem.AppModules[key]; !e {
		nodeItem.AppModules[key] = NewPodCacheAppModuleItem()
	}

	podItem := NewPodCacheItem(app, moduleName, alloc, pod)
	if _, e := nodeItem.AppModules[key].Cache[pod.GetKey()]; e {
		idx := -1
		for i, item := range nodeItem.AppModules[key].List {
			if item.Pod.GetKey() == pod.GetKey() {
				idx = i
				break
			}
		}
		if idx >= 0 {
			remainingPart := nodeItem.AppModules[key].List[idx+1:]
			nodeItem.AppModules[key].List = nodeItem.AppModules[key].List[0:idx]
			nodeItem.AppModules[key].List = append(nodeItem.AppModules[key].List, remainingPart...)
		}
	}
	nodeItem.AppModules[key].List = append(nodeItem.AppModules[key].List, podItem)
	sort.Slice(nodeItem.AppModules[key].List, func(i, j int) bool {
		if nodeItem.AppModules[key].List[j].Allocation.MinimumCapacity.GE(nodeItem.AppModules[key].List[i].Allocation.MinimumCapacity) {
			return true
		}
		return nodeItem.AppModules[key].List[j].Allocation.MaximumCapacity.GE(nodeItem.AppModules[key].List[i].Allocation.MaximumCapacity)
	})
	nodeItem.AppModules[key].Cache[pod.GetKey()] = podItem
	if enqueue {
		if p.QueuingPods == nil {
			p.QueuingPods = make(map[string]*kernel.Pod)
		}
		p.QueuingPods[pod.GetKey()] = pod
	}
	if p.Pods == nil {
		p.Pods = make(map[string]*kernel.Pod)
	}
	p.Pods[pod.GetKey()] = pod
}
