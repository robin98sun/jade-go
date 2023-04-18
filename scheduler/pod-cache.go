package scheduler

import (
	"sync"
	"strings"
	"uta.edu/aces/jade-go/histogram"
	ds "uta.edu/aces/jadesdk/data_structure"
)

type PodCache struct {
	Nodes                      map[string]*PodCacheNodeItem // nodekey: cacheItem
	Pods                       map[string]*ds.Pod
	IsBackgroundRoutineStarted bool
	mutex                      *sync.Mutex
}

func (p *PodCache) Lock() {
	p.mutex.Lock()
}

func (p *PodCache) Unlock() {
	p.mutex.Unlock()
}

func (p *PodCache) DescribeScalablePods() interface{} {

	p.Lock()
	defer p.Unlock()

	result := make(map[string]interface{})

	counted_total_pods := 0
	counted_scalable_pods := 0

	for nodekey, nodeItem := range p.Nodes {
		nodeStat := make(map[string]interface{})
		for appModuleKey, nodeScheduler := range nodeItem.AppModules {
			appStat := make(map[string]interface{})

			counted_total_pods += len(nodeScheduler.Pods)
			counted_scalable_pods += len(nodeScheduler.Queue.Pods)

			appStat["pods"] = len(nodeScheduler.Pods)
			appStat["scalable_pods"] = len(nodeScheduler.Queue.Pods)

			nodeStat[appModuleKey] = appStat
		}
		result[nodekey] = nodeStat
	}

	result["counted_total_pods"] = counted_total_pods
	result["counted_scalable_pods"] = counted_scalable_pods

	result["total_pods"] = len(p.Pods)
	result["scalable_worker_pods"] = len(p.GetSchedulablePods(string(ds.AppModuleWorker)))
	
	return result
}


func (p *PodCache) GetPod(podkey string) *ds.Pod {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Pods != nil && len(p.Pods) > 0 {
		if pod, e := p.Pods[podkey]; e {
			return pod
		}
	}	

	return nil
}

func (p *PodCache) GetPods() []*ds.Pod {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	var result []*ds.Pod

	for _, pod := range p.Pods {
		result = append(result, pod)
	}
	return result
}

func (p *PodCache) Clear() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for key := range p.Nodes {
		delete(p.Nodes, key)
	}

	for key := range p.Pods {
		delete(p.Pods, key)
	}
}

func NewPodCache() *PodCache {
	inst := &PodCache{
		Nodes: make(map[string]*PodCacheNodeItem),
		mutex:     &sync.Mutex{},
	}
	return inst
}

type PodCacheNodeItem struct {
	AppModules map[string]*NodeScheduler // appName+ModuleName: pod instance
}

func NewPodCacheNodeItem() *PodCacheNodeItem {
	inst := &PodCacheNodeItem{
		AppModules: make(map[string]*NodeScheduler),
	}
	return inst
}



func (p *PodCache) GetAllPods() []*ds.Pod {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	pod_list := []*ds.Pod {}
	for _, pod := range p.Pods {
		pod_list = append(pod_list, pod)
	}
	return pod_list
}

func (p *PodCache) GetSchedulablePods(moduleName string) []*ds.Pod {

	pod_list := []*ds.Pod {}
	for _, nodeItem := range p.Nodes {
		for key, nodeScheduler := range nodeItem.AppModules {
			if p.IsKeyForModule(key, string(ds.AppModuleWorker)) {
				nodePods := nodeScheduler.GetSchedulablePods()
				if len(nodePods) > 0 {
					pod_list = append(pod_list, nodePods...)
				}
			}
		}
	}
	return pod_list
}


func (p *PodCache) SetPodIdle(pod *ds.Pod, serviceRequestTime float64, communicationTime float64, queueingTime float64, budget float64) *NodeScheduler {
	return p.setPodIdleOrNot(pod, true, serviceRequestTime, communicationTime, queueingTime, budget)
}
func (p *PodCache) SetPodBusy(pod *ds.Pod) *NodeScheduler {
	return p.setPodIdleOrNot(pod, false, float64(-1), float64(-1), float64(-1), float64(-1))
}
func (p *PodCache) setPodIdleOrNot(pod *ds.Pod, idle bool, serviceRequestTime float64, communicationTime float64, queueingTime float64, budget float64) *NodeScheduler  {

	if p == nil || len(p.Nodes) == 0 || pod == nil {
		return nil
	}
	
	if nodeItem, e := p.Nodes[pod.NodeKey]; e {
		key := p.GetKeyFromApplicationAndModule(pod.AppKey, pod.ModuleName)
		if podItem, e := nodeItem.AppModules[key]; e {
			podItem.SetPodIdle(pod, idle, serviceRequestTime, communicationTime, queueingTime, budget)
			return podItem
		}
	}
	return nil
}
func (p *PodCache) IsPodIdle(pod *ds.Pod) bool {

	if p == nil || len(p.Nodes) == 0 || pod == nil {
		return false
	}

	if nodeItem, e := p.Nodes[pod.NodeKey]; e {
		key := p.GetKeyFromApplicationAndModule(pod.AppKey, pod.ModuleName)
		if podItem, e := nodeItem.AppModules[key]; e {
			return podItem.IsPodIdle(pod)
		}
	}
	return false
}

func (p *PodCache) GetNodeSchedulerForModule(nodeKey string, appKey string, moduleName string, alloc *ds.AllocationUnit) *NodeScheduler {
	if p == nil {
		return nil
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if nodeItem, e := p.Nodes[nodeKey]; e {
		key := p.GetKeyFromApplicationAndModule(appKey, moduleName)
		if nodeScheduler, e := nodeItem.AppModules[key]; e{
			return nodeScheduler
		}
	}
	return nil
}

func (p *PodCache) GetOnePodForModule(nodeKey string, appKey string, moduleName string, alloc *ds.AllocationUnit) *ds.Pod {
	if p == nil {
		return nil
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if nodeItem, e := p.Nodes[nodeKey]; e {
		key := p.GetKeyFromApplicationAndModule(appKey, moduleName)
		if podItem, e := nodeItem.AppModules[key]; e{
			pods := podItem.GetSchedulablePods()
			if len(pods) > 0 {
				return pods[0]
			}
		}
	}
	return nil
}

func (p *PodCache) IsKeyForModule(key string, moduleName string) bool {
	parts := strings.Split(key, "::")
	if len(parts) > 1 {
		if parts[len(parts)-1] == moduleName {
			return true
		}
	}
	return false
}

func (p *PodCache) GetKeyFromApplicationAndModule(appKey string, moduleName string) string {
	return appKey + "::" + moduleName
}

func (p *PodCache) SetPodForApplication(nodeKey string, app *ds.Application, moduleName string, pod *ds.Pod, alloc *ds.AllocationUnit) {
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

	if nodeScheduler, e := nodeItem.AppModules[key]; !e {
		nodeItem.AppModules[key] = NewNodeScheduler(nodeKey, app, moduleName, []*ds.Pod{pod})
	} else {
		nodeScheduler.SetPod(pod, NodeSchedulerPodStatusIdle)
	}

	if alloc != nil {
		pod.Allocation = alloc
	}

	if p.Pods == nil {
		p.Pods = make(map[string]*ds.Pod)
	}
	p.Pods[pod.GetKey()] = pod
}

func (p *PodCache) SetReplicaPerNode(nodeKey string, appKey string, moduleName string, replicaCount int) int {
	// p.LockMeta()
	// defer p.UnlockMeta()
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.Nodes == nil {
		p.Nodes = make(map[string]*PodCacheNodeItem)
	}
	if _, e := p.Nodes[nodeKey]; !e {
		return 0
	}
	nodeItem := p.Nodes[nodeKey]
	key := p.GetKeyFromApplicationAndModule(appKey, moduleName)

	if nodeScheduler, e := nodeItem.AppModules[key]; !e {
		return 0
	} else {
		return nodeScheduler.SetReplicaPerNode(replicaCount)
	}
	return 0

}


func (p *PodCache) CalcTailForNodes(subtasks []*ds.SubtaskOnNode, percentile float64, histType STQueueHistogramType) float64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	histogram_list := []*histogram.Histogram{}

	for _, subtaskOnNode := range subtasks {
		subtask := subtaskOnNode.Subtask
		node := subtaskOnNode.Node
		if nodeItem, e := p.Nodes[node.GetKey()]; e {
			appModuleKey := p.GetKeyFromApplicationAndModule(subtask.AppKey, subtask.ModuleName)
			if nodeScheduler, e := nodeItem.AppModules[appModuleKey]; e {
				if histType == STQueueHistogramTypeServiceResponseTime {
					histogram_list = append(histogram_list, nodeScheduler.Queue.HistogramServiceTime)
				} else if histType == STQueueHistogramTypeServiceResponseTimeWithQueueingTime {
					histogram_list = append(histogram_list, nodeScheduler.Queue.HistogramWithQueueingTime)
				} else if histType == STQueueHistogramTypeAdjustedServiceResponseTime {
					histogram_list = append(histogram_list, nodeScheduler.Queue.HistogramAdjustedServiceTime)
				}
			}
		}
		
	}

	if len(histogram_list) > 0 {
		return histogram.CalcPercentileOfProduct(percentile, histogram_list, false)
	}
	return 0
}

func (p *PodCache) CleanAndResetQueues() {
	p.Lock()
	defer p.Unlock()

	for _, nodeItem := range p.Nodes {
		if len(nodeItem.AppModules) == 0 {
			continue
		}
		for _, podItem := range nodeItem.AppModules {
			podItem.SetAllPodsIdle()
			podItem.Queue.Clean()
		}
	}
}

func (p *PodCache) GetPodUIDsAsPerQueue(appKey string, queueKey string) []string {
	p.Lock()
	defer p.Unlock()

	defaultAppModule := ds.AppModuleWorker
	key := p.GetKeyFromApplicationAndModule(appKey, string(defaultAppModule))
	if nodeItem, e1 := p.Nodes[queueKey]; e1 {
		if nodeScheduler, e2 := nodeItem.AppModules[key]; e2 && nodeScheduler != nil && len(nodeScheduler.Pods) > 0 {

			pods := []string{}
			for _, pod := range nodeScheduler.Pods {
				pods = append(pods, pod.UID)
			}

			return pods

		}
	}
	return nil
}
