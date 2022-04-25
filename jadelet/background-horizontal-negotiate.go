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
			j.log.Printf("got %v eligible neighbors from registry: %v", len(eligibleNeighbors), eligibleNeighbors)
			if eligibleNeighbors == nil {
				eligibleNeighbors = []*kernel.Node{}
			}
			j.eligibleNeighborCache.StoreEligibleNeighbors(query_key, eligibleNeighbors)
		}
		if len(eligibleNeighbors) > 0 {
			j.log.Printf("retreved %v eligible neighbors from cache", len(eligibleNeighbors))

			budgetNegotiation := scheduler.BudgetNegotiationTypeHistogram
			if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiation != "" {
				budgetNegotiation = dispatchItem.Options.BudgetNegotiation
			}

			newDispatchItem := dispatchItem.MinimumCopy()
			newDispatchItem.SetReportToForModule(string(kernel.AppModuleAggregator), j.Config.SelfNode, nil)
			newDispatchItem.Options = &scheduler.TaskDispatchingOptions{
				BudgetNegotiation: budgetNegotiation,
			}
			newDispatchItem.TTL = dispatchItem.TTL - 1

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
	j.log.Printf("all inquiries are done")

	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	// multiply CDFs
	var cdf_list []*histogram.CDF
	for _, res := range cache.Responses {
		if res.Response == nil {
			continue
		}

		cdf_list = append(cdf_list, res.Response.CDF)
	}

	tail_latency := histogram.SearchCDFProduct(cdf_list, float64(0.99))

	j.log.Printf("99 percentile tail latency of %v CDFs is %v", len(cdf_list), tail_latency)

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

	j.log.Printf("inquirying eligible neighbor %v for budget on task %v ", neighbor, sampleTask)
	apiPath := "/$jade$/inquiryBudget"
	_, _, content, err := j.HTTPCommunicate("inquirying eligible neighbor", "POST", apiPath, neighbor, payload, 0, 10)
	if err != nil {
		j.log.Println("ERROR when inquirying eligible neighbor:", err.Error())
	} else {
		resInst :=  &struct{
			Payload *BudgetNegotiationResponse `json:"payload,omitempty"`
		}{}
		err = json.Unmarshal(content, resInst)
		if err != nil {
			j.log.Println("ERROR of inquirying eligible neighbor: can not decode response, ", err)
		} else {
			j.log.Println("response of inquirying eligible neighbor:", resInst.Payload)

			cache.SetResponse(neighbor, resInst.Payload)
			return resInst.Payload
		}
	}
	cache.SetResponse(neighbor, nil)
	return nil
}

func (j *JADE) fetchEligibleAutonomyServiceDomains(query *kernel.Requirements) []*kernel.Node {
	payload := j.GeneratePayloadOfRequest(j.Config.RegistryNode, query, nil, nil)

	j.log.Printf("fetching eligible neighbors from registry node [%v], which is %v empty", j.Config.RegistryNode, j.Config.RegistryNode.IsAddrEmpty())
	apiPath := "/$jade$/eligibleNeighbors"
	_, _, content, err := j.HTTPCommunicate("fetch eligible neighbors", "POST", apiPath, j.Config.RegistryNode, payload, 0, 10)
	if err != nil {
		j.log.Println("ERROR when fetching eligible neighbors:", err.Error())
	} else {
		resInst :=  &struct{
			Payload []*kernel.Node `json:"payload,omitempty"`
		}{}
		err = json.Unmarshal(content, resInst)
		if err != nil {
			j.log.Println("ERROR of fetching eligible neighbors: can not decode response, ", err)
		} else {
			j.log.Println("response of fetching eligible neighbors:", resInst.Payload)
			return resInst.Payload
		}
	}
	return nil
}
