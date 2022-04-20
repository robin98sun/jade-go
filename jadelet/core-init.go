package jadelet

import (
	"sync"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/kube"
	"uta.edu/aces/jade-go/provisioner"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jadesdk"
)

// Init to do initializing work
func (j *JADE) Init() {
	j.log = &kernel.Logger{}
	j.mutex = &sync.Mutex{}
	j.registrationMutex = &sync.Mutex{}
	// Initialize caches and queues
	j.sdk = jadesdk.NewJadeSDK()
	j.Subnodes = make(map[string]*kernel.Node)
	j.Neighbors = make(map[string]*kernel.Node)
	j.subnodeCapabilityCache = kernel.NewCapabilityCache()
	j.subnodeCapacityCache = kernel.NewCapacityCache()
	j.neighborCapabilityCache = kernel.NewCapabilityCache()
	j.neighborCapacityCache = kernel.NewCapacityCache()
	j.CapacityStatus = &kernel.CapacityStatus{}
	j.TaskCache = scheduler.NewTaskCache()
	j.PodCache = scheduler.NewPodCache()
	j.dist = scheduler.NewDist()
	// read environment variables into config
	j.Config = kernel.ReadConfFromEnv()
	j.CapacityStatus.MaximumCapacity = j.Config.Capacity.Copy()
	j.CapacityStatus.RemainingCapacity = j.Config.Capacity.Copy()
	if !j.Config.SelfNode.IsAddrEmpty() {
		j.log.Printf("setting capabilities during initializing")
		j.subnodeCapabilityCache.Set(j.Config.SelfNode.Key(), j.Config.Capabilities)
		j.neighborCapabilityCache.Set(j.Config.SelfNode.Key(), j.Config.Capabilities)
	}
	// read env metrics if the addon is deployed
	
	// setup k8s client instance
	clients := kube.NewKubeClient(j.log)
	clients.Init()
	j.Kube = clients
	j.Provisioner = provisioner.NewProvisioner(j.log)
	// Register to upper node
	if j.Config.SelfNode.IsAddrEmpty() {
		j.MakeUpAddressForNode(j.Config.SelfNode)
	}
	go j.RegisterToNode(JadeNodeTypeUpperNode, int64(0))
	go j.RegisterToNode(JadeNodeTypeRegistryNode, int64(0))
	// go j.routimeForPodQueues(1000)
}

