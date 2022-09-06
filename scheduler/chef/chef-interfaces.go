package chef

import (
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler/histogram"
)


func (c *TakoyakiChef) AllocatePod(nodeKey string, appId string, moduleName string,  resourceId string, capacity *kernel.Capacity) {

	pan := c.LocateOrCreatePan(nodeKey, appId, moduleName)

	cavity := pan.LocateOrCreateCavity(resourceId, capacity)

	cavity.mutex.Lock()
	defer cavity.mutex.Unlock()
	if !cavity.Capacity.EQ(capacity) {
		cavity.Capacity = capacity
	}

}

func (c *TakoyakiChef) RevokePod(nodeKey string, appId string, moduleName string,  resourceId string) {

	pan := c.LocateOrCreatePan(nodeKey, appId, moduleName)

	pan.LocateAndVoidCavity(resourceId)
}


func (c *TakoyakiChef) CalcTailForNodes(nodes []string, appId string, moduleName string, percentile float64, histType QueueHistogramType) float64 {

	histogram_list := []*histogram.Histogram{}

	for _, nodeKey := range nodes {
		pan := c.FindPan(nodeKey, appId, moduleName)
		if pan != nil {
			if histType == QueueHistogramTypeServiceResponseTime {
				histogram_list = append(histogram_list, pan.Queue.HistogramServiceTime)
			} else if histType == QueueHistogramTypeServiceResponseTimeWithQueueingTime {
				histogram_list = append(histogram_list, pan.Queue.HistogramWithQueueingTime)
			} else if histType == QueueHistogramTypeAdjustedServiceResponseTime {
				histogram_list = append(histogram_list, pan.Queue.HistogramAdjustedServiceTime)
			}
		}
	}

	if len(histogram_list) > 0 {
		return histogram.CalcPercentileOfProduct(percentile, histogram_list, false)
	}

	return 0
}

func (c *TakoyakiChef) CleanAndResetQueues() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, stove := range c.Stoves {
		stove.mutex.Lock()
		defer stove.mutex.Unlock()
		for _, pan := range stove.Pans {
			pan.mutex.Lock()
			defer pan.mutex.Unlock()

			pan.Queue.Clean()
			for _, cavity := range pan.Cavities {
				cavity.mutex.Lock()
				defer cavity.mutex.Unlock()

				cavity.TakoyakiID = ""
			}
		}
	}
}

func (c *TakoyakiChef) Enqueue(
	nodeKey string, appId string, moduleName string,
	targetQueue QueueType, taskKey string, subtaskKey string, payload interface{}, 
	queueingMechanism kernel.TaskQueuingMechanism, maxQueuingTime float64, 
	priority int, estimatedServiceTime float64, // milliseconds
	minCapReq *kernel.Capacity,
	printf func(string, ...interface{}),
) (bool, *QueueItem, int) {

	pan := c.LocateOrCreatePan(nodeKey, appId, moduleName)
	
	return pan.Enqueue(
		targetQueue, taskKey, subtaskKey, payload, queueingMechanism,
		maxQueuingTime, priority, estimatedServiceTime, minCapReq, printf,
	)
}


func (c *TakoyakiChef) SearchNodesToAccommodate(appId string, moduleName string, minCapReq *kernel.Capacity) []string {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	nodes := []string{}
	for nodeKey, stove := range c.Stoves {
		stove.mutex.Lock()
		defer stove.mutex.Unlock()
		for _, pan := range stove.Pans {
			pan.mutex.Lock()
			defer pan.mutex.Unlock()
			found := false
			if pan.ApplicationID == appId && pan.ModuleName == moduleName {
				for _, cavity := range pan.Cavities {
					if cavity.Capacity.GE(minCapReq) {
						nodes = append(nodes, nodeKey)
						found = true
						break
					}
				}
			}
			if found {break}
		}
	}

	return nodes
}

