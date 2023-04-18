package scheduler

import (
	// "math"
	"sync"
	// "strings"
	// "uta.edu/aces/jade-go/histogram"
	ds "uta.edu/aces/jadesdk/data_structure"
	"math/rand"
	"time"
)

type NodeSchedulerPolicy string
const(
	NodeSchedulerPolicyRoundRobin NodeSchedulerPolicy = "round-robin"
)

type NodeSchedulerPodStatus string
const(
	NodeSchedulerPodStatusIdle NodeSchedulerPodStatus = "idle"
	NodeSchedulerPodStatusBusy NodeSchedulerPodStatus = "busy"
	NodeSchedulerPodStatusPending NodeSchedulerPodStatus = "pending"
	NodeSchedulerPodStatusTimeout NodeSchedulerPodStatus = "timeout"
)

type NodeScheduler struct {
	Application *ds.Application
	Queue       *STQueue
	ModuleName  string
	PodList     []*ds.Pod
	Pods        map[string]*ds.Pod
	PodsStatus  map[string]NodeSchedulerPodStatus
	NodeKey     string
	Policy      NodeSchedulerPolicy
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
		PodList:     []*ds.Pod{},
		Pods:        make(map[string]*ds.Pod),
		PodsStatus:  make(map[string]NodeSchedulerPodStatus),
		NodeKey:     nodeKey,
		Policy: 	 NodeSchedulerPolicyRoundRobin,
		mutex: 		 &sync.Mutex{},
	}
	
	for _, pod := range pods {
		inst.SetPod(pod, NodeSchedulerPodStatusIdle)
	}
	return inst
}

func (n *NodeScheduler) SetPod(pod *ds.Pod, status NodeSchedulerPodStatus) {
	if pod == nil {return}
	if n == nil {return}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	if _, e := n.Pods[pod.GetKey()]; !e {
		n.PodList = append(n.PodList, pod)
		n.Pods[pod.GetKey()] = pod
	}
	n.PodsStatus[pod.GetKey()] = status

}

func (n *NodeScheduler) GetSchedulablePods() []*ds.Pod {
	if n == nil {return nil}

	n.mutex.Lock()
	defer n.mutex.Unlock()
	
	schedulablePods := []*ds.Pod{}
	for podKey, status := range n.PodsStatus {
		if status == NodeSchedulerPodStatusIdle {
			schedulablePods = append(schedulablePods, n.Pods[podKey])
		}
	}
	if n.Policy == NodeSchedulerPolicyRoundRobin {
		if len(schedulablePods) == 1 {
			return schedulablePods
		} else if len(schedulablePods) > 1 {
			result := []*ds.Pod{}
			rand.Seed(time.Now().UnixNano())
			index := rand.Intn(len(schedulablePods))
			result = append(result, schedulablePods[index])
			return result
		}
	}
	
	return schedulablePods
}

func (n *NodeScheduler) SetReplicaPerNode(replicaCount int) int {
	// yet to implemented 

	return replicaCount
}

func (n *NodeScheduler) GetReplicaPerNode() int {

	n.mutex.Lock()
	n.mutex.Unlock()

	return len(n.PodList)
}

func (n *NodeScheduler) GetPodByIndex(podIndex int) *ds.Pod {

	n.mutex.Lock()
	defer n.mutex.Unlock()

	if podIndex>=0 && podIndex < len(n.PodList) {
		return n.PodList[podIndex]
	}
	return nil
}


func (n *NodeScheduler) IsPodIdle(pod *ds.Pod) bool {

	n.mutex.Lock()
	defer n.mutex.Unlock()

	if podStatus, e := n.PodsStatus[pod.GetKey()]; e {
		return podStatus == NodeSchedulerPodStatusIdle
	} else if _, e := n.Pods[pod.GetKey()]; e {
		n.PodsStatus[pod.GetKey()] = NodeSchedulerPodStatusIdle
		return true
	}

	return false
}

func (n *NodeScheduler) SetAllPodsIdle() {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	for _, pod := range n.PodList {
		n.PodsStatus[pod.GetKey()] = NodeSchedulerPodStatusIdle
	}
}

func (n *NodeScheduler) SetPodIdle(pod *ds.Pod, idle bool, serviceRequestTime float64, communicationTime float64, queueingTime float64, budget float64) {

	if pod == nil || n == nil {return}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	if _, e := n.Pods[pod.GetKey()]; !e {return}

	if idle {
		n.PodsStatus[pod.GetKey()] = NodeSchedulerPodStatusIdle

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
	} else {
		n.PodsStatus[pod.GetKey()] = NodeSchedulerPodStatusBusy
	}

}


