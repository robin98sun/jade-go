package jadelet

import (
	"aces/jade-go/kernel"
	"aces/jade-go/kube"
	"aces/jade-go/provisioner"
	"aces/jade-go/scheduler"
	"sync"
)

// Init to do initializing work
func (j *JADE) Init() {
	j.log = &kernel.Logger{}
	j.mutex = &sync.Mutex{}
	// Initialize caches and queues
	j.Subnodes = make(map[string]*kernel.Node)
	j.capabilityCache = &kernel.CapabilityCache{}
	j.capacityCache = &kernel.CapacityCache{}
	j.CapacityStatus = &kernel.CapacityStatus{}
	j.TaskCache = scheduler.NewTaskCache()
	j.PodCache = scheduler.NewPodCache()
	// read environment variables into config
	j.Config = kernel.ReadConfFromEnv()
	j.CapacityStatus.MaximumCapacity = j.Config.Capacity.Copy()
	j.CapacityStatus.RemainingCapacity = j.Config.Capacity.Copy()
	// setup k8s client instance
	clients := kube.NewKubeClient(j.log)
	clients.Init()
	j.Kube = clients
	j.Provisioner = provisioner.NewProvisioner(j.log)
	// Register to upper node
	if j.Config.SelfNode.IsAddrEmpty() {
		j.MakeUpAddressForNode(j.Config.SelfNode)
	}
	go j.Register(0)
	go j.routimeForPodQueues()
}
