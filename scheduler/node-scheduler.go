package scheduler

import (
	// "math"
	"sync"
	// "strings"
	// "uta.edu/aces/jade-go/histogram"
	ds "uta.edu/aces/jadesdk/data_structure"
)

type NodeSchedulerPolicy string
const(
	NodeSchedulerPolicyRoundRobin NodeSchedulerPolicy = "round-robin"
)

type NodeScheduler struct {
	Application *ds.Application
	Queue       *STQueue
	ModuleName  string
	Pods        []*ds.Pod
	NodeKey     string
	IsIdle      bool
	mutex 		*sync.Mutex
}

func (i *NodeScheduler) IsEmpty() bool {
	if i == nil || len(i.Pods) == 0 {
		return true
	}
	return false
}

func NewNodeScheduler(nodeKey string, app *ds.Application, moduleName string, pods []*ds.Pod) *NodeScheduler {
	inst := &NodeScheduler{
		Application: app,
		ModuleName:  moduleName,
		Queue:       NewSTQueue(nodeKey),
		Pods:        pods,
		IsIdle:      true,
		NodeKey:     nodeKey,
		mutex: 		 &sync.Mutex{},
	}
	inst.Queue.Pods = pods
	return inst
}

func (n *NodeScheduler) GetSchedulablePods() []*ds.Pod {
	if n == nil {return nil}

	return n.Pods
}


func (n *NodeScheduler) IsPodIdle(pod *ds.Pod) bool {

	return n.IsIdle
}

func (n *NodeScheduler) SetAllPodsIdle() {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.IsIdle = true
}

func (n *NodeScheduler) SetPodIdle(pod *ds.Pod, idle bool, serviceRequestTime float64, communicationTime float64, queueingTime float64, budget float64) {

	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.IsIdle = idle
	if n.Queue.HistogramServiceTime != nil && serviceRequestTime >= 0 {
		n.Queue.HistogramServiceTime.Enqueue(serviceRequestTime, 1)
	}
	if n.Queue.HistogramWithQueueingTime != nil && queueingTime >= 0 && serviceRequestTime >= 0 {
		n.Queue.HistogramWithQueueingTime.Enqueue(serviceRequestTime+queueingTime, 1)	
	}
	if n.Queue.HistogramAdjustedServiceTime != nil && budget >= 0 && queueingTime >= 0 && serviceRequestTime >= 0 {
		adjustedServiceTime := serviceRequestTime+queueingTime-budget
		if adjustedServiceTime < 0 {
			adjustedServiceTime = 0
		}
		n.Queue.HistogramAdjustedServiceTime.Enqueue(adjustedServiceTime, 1)	
	}
	if n.Queue.HistogramCommunicationTime != nil && communicationTime >= 0 {
		n.Queue.HistogramCommunicationTime.Enqueue(communicationTime, 1)
	}

}


