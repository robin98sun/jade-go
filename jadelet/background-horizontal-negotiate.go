package jadelet

import (
	"time"
	// "sort"
	"encoding/json"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	// "uta.edu/aces/jade-go/histogram"
	// "uta.edu/aces/jadesdk"
	"sync"
)

func (j *JADE) evaluateCollaborativeTasks(tasklist map[string]*scheduler.TaskDispatchingItem) {
	for _, dispatchItem := range tasklist {
		if dispatchItem.Task == nil || dispatchItem.Task.Requirements == nil {
			continue
		}
		query := dispatchItem.Task.Requirements
		query_key := query.GetQueryKey()
		if query_key == "" {
			continue
		}

		eligibleNeighbors := j.eligibleNeighborCache.GetEligibleNeighbors(query_key)

		if eligibleNeighbors == nil {
			eligibleNeighbors = j.fetchEligibleAutonomyServiceDomains(query)
			j.log.Printf("[budget negotiation] got %v eligible neighbors from registry: %v", len(eligibleNeighbors), eligibleNeighbors)
			if eligibleNeighbors == nil {
				eligibleNeighbors = []*kernel.Node{}
			}
			j.eligibleNeighborCache.StoreEligibleNeighbors(query_key, eligibleNeighbors)
		}
		if len(eligibleNeighbors) > 0 {
			j.log.Printf("[budget negotiation] retreved %v eligible neighbors from cache", len(eligibleNeighbors))

			budgetNegotiation := scheduler.BudgetNegotiationTypeHistogram
			if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiation != "" {
				budgetNegotiation = dispatchItem.Options.BudgetNegotiation
			}

			newDispatchItem := dispatchItem.MinimumCopy()
			newDispatchItem.SetReportToForModule(string(kernel.AppModuleAggregator), j.Config.SelfNode.GetSDKNode(), nil)
			newDispatchItem.Options = &scheduler.TaskDispatchingOptions{
				BudgetNegotiation: budgetNegotiation,
			}
			if dispatchItem.Options != nil && dispatchItem.Options.CDFPoints > 0 {
				newDispatchItem.Options.CDFPoints = dispatchItem.Options.CDFPoints
			}
			if dispatchItem.Options != nil && dispatchItem.Options.CDFStartPoint > 0 && dispatchItem.Options.CDFStartPoint <= 1 {
				newDispatchItem.Options.CDFStartPoint = dispatchItem.Options.CDFStartPoint
			}
			newDispatchItem.TTL = dispatchItem.TTL - 1

			//  for other types of estimations
			if budgetNegotiation != scheduler.BudgetNegotiationTypeHistogram {

			}

			cache := NewBudgetNegotiationResponseCache()
			for _, neighbor := range eligibleNeighbors {
				cache.Responses[neighbor.Key()] = &BudgetNegotiationResponseCacheItem{
					Neighbor: neighbor,
					IsDone: false,
					requestSentAt: time.Now(),
				}
			}
			dispatchItem.InquiryStartTimestamp = time.Now()
			for _, neighbor := range eligibleNeighbors {
				go j.inquiryBudget(neighbor, newDispatchItem, cache)
			}
			go j.CallbackOfNegotiation(cache, dispatchItem)

		}

	}
}

func (j *JADE) CallbackOfNegotiation(cache *BudgetNegotiationResponseCache, dispatchItem *scheduler.TaskDispatchingItem) {
	for {
		time.Sleep(1 * time.Millisecond)

		isDone := true
		cache.mutex.Lock()
		for _, cacheItem := range cache.Responses {
			if !cacheItem.IsDone {
				isDone = false
				break
			}
		}
		cache.mutex.Unlock()

		if isDone {
			break
		}
	}

	// 
	j.log.Printf("[budget negotiation] all inquiries are done")
	dispatchItem.InquiryDoneTimestamp = time.Now()

	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	// multiply CDFs
	var cdf_list []*histogram.CDF
	for _, cacheItem := range cache.Responses {
		if cacheItem.Response == nil || cacheItem.Response.CDF == nil {
			continue
		}

		cdf_list = append(cdf_list, cacheItem.Response.CDF)
		dispatchItem.Task.CreateSubtask(
			string(kernel.AppModuleAggregator), 
			cacheItem.Neighbor.Key(),
			"", "", false,
		)
	}


	tailLatencySLO := float64(1000)
	if dispatchItem.SLO != nil {
		tailLatencySLO = dispatchItem.SLO.TailLatencyInMilliseconds
	}
	budget := tailLatencySLO
	negotiationOverhead := float64(0)

	if len(cdf_list) > 0 {
		budgetEstimationPercentilePoint := float64(0.99)
		if dispatchItem.Options != nil && dispatchItem.Options.BudgetEstimationPercentilePoint > 0 && dispatchItem.Options.BudgetEstimationPercentilePoint <= 1 {
			budgetEstimationPercentilePoint = dispatchItem.Options.BudgetEstimationPercentilePoint
		}
		tail_latency := histogram.SearchCDFProduct(cdf_list, budgetEstimationPercentilePoint)

		j.log.Printf("[budget negotiation] 99 percentile tail latency of %v CDFs is %v", len(cdf_list), tail_latency)

		negotiationOverhead := float64(dispatchItem.BudgetEstimationDoneTimestamp.Sub(dispatchItem.InquiryStartTimestamp) * 10 / time.Millisecond) /10

		if tail_latency < tailLatencySLO {
			budget = tailLatencySLO - tail_latency - negotiationOverhead
		} else {
			budget = 0
		}

		dispatchItem.SetBudgetForModule(string(kernel.AppModuleWorker), budget)
	}

	dispatchItem.BudgetEstimationDoneTimestamp = time.Now()

	j.log.Printf("[budget negotiation] budget negotiation done in %v milliseconds, budget estimation done in %v milliseconds",
		dispatchItem.InquiryDoneTimestamp.Sub(dispatchItem.InquiryStartTimestamp) / time.Millisecond,
		dispatchItem.BudgetEstimationDoneTimestamp.Sub(dispatchItem.InquiryDoneTimestamp) / time.Millisecond,
	)

	j.log.Printf("[budget negotiation] tail latency SLO: %v, estimated budget: %v, deducted budget negotiation overhead: %v milliseconds", tailLatencySLO, budget, negotiationOverhead )
	j.log.Printf("[budget negotiation] going to dispatch the task among all eligible clusters")
	j.evaluateAggregativeTasks(map[string]*scheduler.TaskDispatchingItem{
		dispatchItem.Task.GetKey(): dispatchItem,
	})

	// 

}


type BudgetNegotiationResponse struct {
	AvailableNodes int64 `json:"availableNodes,omitempty"`
	CDF *histogram.CDF 	 `json:"cdf,omitempty"`
}

type BudgetNegotiationResponseCacheItem struct {
	Neighbor 			*kernel.Node
	requestSentAt 		time.Time
	responseArriveAt 	time.Time
	Response 			*BudgetNegotiationResponse
	IsDone              bool
}

type BudgetNegotiationResponseCache struct {
	Responses map[string]*BudgetNegotiationResponseCacheItem
	mutex *sync.Mutex
}

func NewBudgetNegotiationResponseCache() *BudgetNegotiationResponseCache {
	return &BudgetNegotiationResponseCache{
		Responses: make(map[string]*BudgetNegotiationResponseCacheItem),
		mutex: &sync.Mutex{},
	}
}

func (c *BudgetNegotiationResponseCache) SetResponse(neighbor *kernel.Node, response *BudgetNegotiationResponse) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if cacheItem, e := c.Responses[neighbor.Key()]; !e {
		c.Responses[neighbor.Key()] = &BudgetNegotiationResponseCacheItem{
			Neighbor: 	neighbor,
			Response: 	response,
			IsDone: 	true,
			responseArriveAt: time.Now(),
		}
	} else {
		cacheItem.Response = response
		cacheItem.IsDone = true
		cacheItem.responseArriveAt = time.Now()
	}
}


func (j *JADE) inquiryBudget(neighbor *kernel.Node, sampleTask *scheduler.TaskDispatchingItem, cache *BudgetNegotiationResponseCache) *BudgetNegotiationResponse {
	payload := j.GeneratePayloadOfRequest(neighbor, sampleTask, nil, nil)

	j.log.Printf("[budget negotiation] inquirying eligible neighbor %v for budget on task %v ", neighbor, sampleTask)
	apiPath := "/$jade$/inquiryBudget"
	_, _, content, err := j.HTTPCommunicate("inquirying eligible neighbor", "POST", apiPath, neighbor, payload, 0, 10)
	if err != nil {
		j.log.Println("[budget negotiation] ERROR when inquirying eligible neighbor:", err.Error())
	} else {
		resInst :=  &struct{
			Payload *BudgetNegotiationResponse `json:"payload,omitempty"`
		}{}
		err = json.Unmarshal(content, resInst)
		if err != nil {
			j.log.Println("[budget negotiation] ERROR of inquirying eligible neighbor: can not decode response, ", err)
		} else {
			j.log.Println("[budget negotiation] response of inquirying eligible neighbor:", resInst.Payload)

			cache.SetResponse(neighbor, resInst.Payload)
			return resInst.Payload
		}
	}
	cache.SetResponse(neighbor, nil)
	return nil
}

func (j *JADE) fetchEligibleAutonomyServiceDomains(query *kernel.Requirements) []*kernel.Node {
	payload := j.GeneratePayloadOfRequest(j.Config.RegistryNode, query, nil, nil)

	j.log.Printf("[budget negotiation] fetching eligible neighbors from registry node [%v], which is %v empty", j.Config.RegistryNode, j.Config.RegistryNode.IsAddrEmpty())
	apiPath := "/$jade$/eligibleNeighbors"
	_, _, content, err := j.HTTPCommunicate("fetch eligible neighbors", "POST", apiPath, j.Config.RegistryNode, payload, 0, 10)
	if err != nil {
		j.log.Println("[budget negotiation] ERROR when fetching eligible neighbors:", err.Error())
	} else {
		resInst :=  &struct{
			Payload []*kernel.Node `json:"payload,omitempty"`
		}{}
		err = json.Unmarshal(content, resInst)
		if err != nil {
			j.log.Println("[budget negotiation] ERROR of fetching eligible neighbors: can not decode response, ", err)
		} else {
			j.log.Println("[budget negotiation] response of fetching eligible neighbors:", resInst.Payload)
			return resInst.Payload
		}
	}
	return nil
}
