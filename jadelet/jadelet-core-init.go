package jadelet

import (
	"sync"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/kube"
	"uta.edu/aces/jade-go/provisioner"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jade-go/perfstat"
	"uta.edu/aces/jadesdk"
	ds "uta.edu/aces/jadesdk/data_structure"
	rm "uta.edu/aces/jade-go/resource_manager"
)

// Init to do initializing work
func (j *JADE) Init() {
	j.log = kernel.NewLogger()
	j.log.Op.Enabled = true
	j.log.Debug.Enabled = true
	j.log.Perf.Enabled = true
	j.log.Heartbeat.Enabled = true
	
	j.mutex = &sync.Mutex{}
	j.registryMutex = &sync.Mutex{}
	// Initialize caches and queues
	j.sdk = jadesdk.NewJadeSDK()
	j.Subnodes = make(map[string]*ds.Node)
	j.Neighbors = make(map[string]*ds.Node)
	j.subnodeCapabilityCache = kernel.NewCapabilityCache()
	// j.subnodeCapacityCache = kernel.NewCapacityCache()
	j.neighborCapabilityCache = kernel.NewCapabilityCache()
	// j.neighborCapacityCache = kernel.NewCapacityCache()
	j.eligibleNeighborCache = kernel.NewEligibleNeighborCache()
	// j.CapacityStatus = &kernel.CapacityStatus{}
	j.TaskCache = scheduler.NewTaskCache()
	j.PodCache = scheduler.NewPodCache()
	
	// Control Loop: monitoring, analyzing, planning, executing
	clock := perfstat.NewClock()

	j.ControlLoop = rm.NewControlLoop(clock)

	msgrAvgSLORatios := func(m *perfstat.PerfMessage) {
		j.ControlLoop.AppendPerfMessage(m)
	}
	j.PerfCache = perfstat.NewPerfCache(clock)
	j.PerfCache.SubscribeAverageSLORatios(msgrAvgSLORatios)

	msgrQueueDV := func(appKey string, deadlineViolationThreshold float64) []*perfstat.QueuePerfMessage {
		return j.PerfCache.GetQueuesAsPerDeadlineViolation(appKey, deadlineViolationThreshold)
	}
	j.ControlLoop.MessengerQueuesAsPerDeadlineViolation = &msgrQueueDV

	msgrQueueDS := func(appKey string, deadlineSurplusThreshold float64) []*perfstat.QueuePerfMessage {
		return j.PerfCache.GetQueuesAsPerDeadlineSurplus(appKey, deadlineSurplusThreshold)
	}
	j.ControlLoop.MessengerQueuesAsPerDeadlineSurplus = &msgrQueueDS

	msgrScaleQueue := func(appKey string, queueKey string, action *rm.ScalingAction) bool {
		node := j.GetNodeInControl(queueKey)
		if node == nil {return false}
		action.SourceNode = j.Config.SelfNode
		return j.CommScaleResource(node, action)
	}
	j.ControlLoop.MessengerScaleQueue = &msgrScaleQueue

	msgrReportScalingResult := func(node *ds.Node, result *rm.ScalingResult) bool {
		return j.CommReportResourceScalingResult(node, result)
	}
	j.ControlLoop.MessengerReportScalingResult = &msgrReportScalingResult

	// others
	j.dist = scheduler.NewDist()
	// read environment variables into config
	j.Config = ds.ReadConfFromEnv()
	// j.CapacityStatus.MaximumCapacity = j.Config.Capacity.Copy()
	// j.CapacityStatus.RemainingCapacity = j.Config.Capacity.Copy()

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

