package scheduler

import (
	"sort"
	"sync"
	"uta.edu/aces/jade-go/kernel"
)

type PodCache struct {
	Nodes                      map[string]*PodCacheNodeItem // nodekey: cacheItem
	dataMutex                  *sync.Mutex
	metaMutex				   *sync.Mutex
	Pods                       map[string]*kernel.Pod
	QueuingPods                map[string]*kernel.Pod
	IsBackgroundRoutineStarted bool
	mutex                      *sync.Mutex
}

func (p *PodCache) LockData() {
	p.dataMutex.Lock()
}

func (p *PodCache) UnlockData() {
	p.dataMutex.Unlock()
}

func (p *PodCache) LockMeta() {
	p.metaMutex.Lock()
}

func (p *PodCache) UnlockMeta() {
	p.metaMutex.Unlock()
}

func (p *PodCache) Clear() {
	// p.LockData()
	// defer p.UnlockData()
	// p.LockMeta()
	// defer p.UnlockMeta()

	p.mutex.Lock()
	defer p.mutex.Unlock()

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
		dataMutex: &sync.Mutex{},
		metaMutex: &sync.Mutex{},
		mutex:     &sync.Mutex{},
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


func (p *PodCache) GetAllPods() []*kernel.Pod {
	// p.LockMeta()
	// defer p.UnlockMeta()
	p.mutex.Lock()
	defer p.mutex.Unlock()

	pod_list := []*kernel.Pod {}
	for _, pod := range p.Pods {
		pod_list = append(pod_list, pod)
	}
	return pod_list
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

func (p *PodCache) SetPodIdle(pod *kernel.Pod, serviceRequestTime float64, communicationTime float64) *PodCacheItem {
	return p.setPodIdleOrNot(pod, true, serviceRequestTime, communicationTime)
}
func (p *PodCache) SetPodBusy(pod *kernel.Pod) *PodCacheItem {
	return p.setPodIdleOrNot(pod, false, float64(-1), float64(-1))
}
func (p *PodCache) setPodIdleOrNot(pod *kernel.Pod, idle bool, serviceRequestTime float64, communicationTime float64) *PodCacheItem  {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// p.LockMeta()
	if p == nil || len(p.Nodes) == 0 || pod == nil {
		// p.UnlockMeta()
		return nil
	}
	
	if nodeItem, e := p.Nodes[pod.NodeKey]; e {
		key := p.GetKeyFromApplicationAndModule(pod.AppKey, pod.ModuleName)
		if appModuleItem, e := nodeItem.AppModules[key]; e && len(appModuleItem.List) > 0 {
			if podItem, e := appModuleItem.Cache[pod.GetKey()]; e {
				// p.UnlockMeta()
				// p.LockData()
				podItem.IsIdle = idle
				if serviceRequestTime >= 0 {
					podItem.Queue.HistogramServiceTime.Enqueue(serviceRequestTime, 1)
				}
				if communicationTime >= 0 {
					podItem.Queue.HistogramCommunicationTime.Enqueue(communicationTime, 1)
				}
				// p.UnlockData()
				return podItem
			}
		}
	}
	// p.UnlockMeta()
	return nil
}
func (p *PodCache) IsPodIdle(pod *kernel.Pod) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// p.LockData()
	// defer p.UnlockData()
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
	// p.LockMeta()
	// defer p.UnlockMeta()

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if nodeItem, e := p.Nodes[nodeKey]; e {
		key := p.GetKeyFromApplicationAndModule(app.Key(), moduleName)
		if appModuleItem, e := nodeItem.AppModules[key]; e && len(appModuleItem.List) > 0 {
			for _, podItem := range appModuleItem.List {
				if alloc == nil || alloc.MinimumCapacity == nil || podItem.Allocation.MinimumCapacity.GE(alloc.MinimumCapacity) {
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

	// p.LockData()
	// defer p.UnlockData()

	p.mutex.Lock()
	defer p.mutex.Unlock()

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
	// p.LockMeta()
	// defer p.UnlockMeta()
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
