package jadelet

import (
	"sync"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/kube"
	"uta.edu/aces/jade-go/provisioner"
	// "uta.edu/aces/scheduler"
	"uta.edu/aces/jadesdk"
	ds "uta.edu/aces/jadesdk/data_structure"
<<<<<<< HEAD
	rm "uta.edu/aces/resource_manager"
=======
>>>>>>> refactoring
)

// Init to do initializing work
func (j *JADE) Init() {
	j.log = kernel.NewLogger()
	j.log.Op.Enabled = true
	
	j.mutex = &sync.Mutex{}
	j.registryMutex = &sync.Mutex{}
	// Initialize caches and queues
	j.sdk = jadesdk.NewJadeSDK()
	j.Subnodes = make(map[string]*ds.Node)
	j.Neighbors = make(map[string]*ds.Node)
	j.subnodeCapabilityCache = kernel.NewCapabilityCache()
<<<<<<< HEAD
	// j.subnodeCapacityCache = ds.NewCapacityCache()
	j.neighborCapabilityCache = kernel.NewCapabilityCache()
	// j.neighborCapacityCache = ds.NewCapacityCache()
	j.eligibleNeighborCache = kernel.NewEligibleNeighborCache()
	j.CapacityStatus = &kernel.CapacityStatus{}

	// j.TaskCache = scheduler.NewTaskCache()
	// j.PodCache = scheduler.NewPodCache()
	// j.PerfCache = perfstat.NewPerfCache()
	// j.dist = scheduler.NewDist()

	// resource manager
	j.ResourceManager = rm.NewResourceManager()

	// scheduler
	// var subtaskDispatcher scheduler.SubtaskDispatcher = func(
	// 	appId string, 
	// 	moduleName string, 
	// 	addr *ds.Node, 
	// 	payload interface{},
	// ) int {
	// 	_, reqlen, _, _ := j.HTTPCommunicate(
	// 		"dispatch subtask "+moduleName, "POST", "/"+moduleName,
	// 		addr,
	// 		payload,
	// 		0, 10,
	// 	)
	// 	return reqlen
	// }

	// var aggregatorTaskDispatcher scheduler.AggregativeTaskDispatcher = func(
	// 	moduleName string, queueKey string, addr *ds.Node, msg interface{},
	// ) int {
	// 	_, reqlen, _, _ := j.HTTPCommunicate(
	// 		"dispatch subtask "+moduleName, "PUT", "/$jade$/enqueueAggregativeTask",
	// 		addr, msg,
	// 		0, 10,
	// 	)
	// 	return reqlen
	// }

	// var neighborTaskDispatcher scheduler.NeighborTaskDispatcher = func(
	// 	neighborNode *ds.Node, dispatchItem *ds.TaskDispatchingItem,
	// ) {
	// 	j.dispatchNeighborTask(neighborNode, dispatchItem)
	// }

	// j.Scheduler = scheduler.NewScheduler(
	// 	j.Config.SelfNode, 50000, 
	// 	subtaskDispatcher, 
	// 	aggregatorTaskDispatcher,
	// 	neighborTaskDispatcher,
	// 	j.log.Op.Printf,
	// )

	// read environment variables into config
	j.Config = ds.ReadConfFromEnv()
	j.CapacityStatus.MaximumCapacity = j.Config.Capacity.Copy()
	j.CapacityStatus.RemainingCapacity = j.Config.Capacity.Copy()
=======
	// j.subnodeCapacityCache = kernel.NewCapacityCache()
	j.neighborCapabilityCache = kernel.NewCapabilityCache()
	// j.neighborCapacityCache = kernel.NewCapacityCache()
	j.eligibleNeighborCache = kernel.NewEligibleNeighborCache()
	// j.CapacityStatus = &kernel.CapacityStatus{}
	j.TaskCache = scheduler.NewTaskCache()
	j.PodCache = scheduler.NewPodCache()
	j.PerfCache = perfstat.NewPerfCache()
	j.dist = scheduler.NewDist()
	// read environment variables into config
	j.Config = ds.ReadConfFromEnv()
	// j.CapacityStatus.MaximumCapacity = j.Config.Capacity.Copy()
	// j.CapacityStatus.RemainingCapacity = j.Config.Capacity.Copy()
>>>>>>> refactoring

	// read env metrics if the addon is deployed
	
	// setup k8s client instance
	clients := kube.NewKubeClient(j.log.Op)
	clients.Init()
	j.Kube = clients
	j.Provisioner = provisioner.NewProvisioner(j.log)
	// Register to upper node
	if j.Config.SelfNode.IsAddrEmpty() {
		j.MakeUpAddressForNode(j.Config.SelfNode)
	}
	j.log.Op.Printf("[init] self node [%v] config emptyness is %v", j.Config.SelfNode.Desc(), j.Config.SelfNode.IsAddrEmpty())
	if !j.Config.SelfNode.IsAddrEmpty() {
		j.log.Op.Printf("[init] setting capabilities during initializing")
		if list, e := j.Config.Capabilities["public"]; e {
			j.subnodeCapabilityCache.Set(j.Config.SelfNode.Key(), list)
			j.neighborCapabilityCache.Set(j.Config.SelfNode.Key(), list)
		}
	}
	go j.RegisterToNode(JadeNodeTypeUpperNode, int64(0))
	go j.RegisterToNode(JadeNodeTypeRegistryNode, int64(0))
	// go j.routimeForPodQueues(1000)
}

func (j *JADE) SetVerboseAccordingToConf() {
	if j.Config != nil && j.Config.Options != nil {

	}
}

