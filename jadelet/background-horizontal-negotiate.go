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
	"math"
	// "sync"
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

		if len(eligibleNeighbors) == 0 {
			eligibleNeighbors = j.fetchEligibleAutonomyServiceDomains(query)
			j.log.Printf("[budget negotiation] got %v eligible neighbors from registry: %v", len(eligibleNeighbors), eligibleNeighbors)
			if eligibleNeighbors == nil {
				eligibleNeighbors = []*kernel.Node{}
			}
			j.eligibleNeighborCache.StoreEligibleNeighbors(query_key, eligibleNeighbors)
		}
		
		dispatchItem.InquiryStartTimestamp = time.Now()
		var budgetnegotationCache *scheduler.BudgetNegotiationResponseCache
		to_cache_neighbor_subtask := true
		if len(eligibleNeighbors) > 0 {
			j.log.Printf("[budget negotiation] retrieved %v eligible neighbors from cache", len(eligibleNeighbors))
			// for some options, no need to negotiate budget

			if 	dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL ||
				dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_None ||
				dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block || 
				dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {

				budgetNegotiation := scheduler.BudgetNegotiationTypeNone
				// if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiation != "" {
				// 	budgetNegotiation = dispatchItem.Options.BudgetNegotiation
				// }
				if dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
					budgetNegotiation = scheduler.BudgetNegotiationTypeCDFNonBlock
				} else if dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block {
					budgetNegotiation = scheduler.BudgetNegotiationTypeCDFBlock
				}

				budgetEstimationPercentilePoint := float64(0.95)
				if dispatchItem.Options != nil && dispatchItem.Options.BudgetEstimationPercentilePoint > 0 && dispatchItem.Options.BudgetEstimationPercentilePoint <= 1 {
					budgetEstimationPercentilePoint = dispatchItem.Options.BudgetEstimationPercentilePoint
				}

				// for single fanout, no need to negotiate
				if len(eligibleNeighbors) == 1 {
					dispatchItem.Task.QueuingMechanism = kernel.TaskQueuingDDL_None
					// if dispatchItem.Options != nil  {
					// 	dispatchItem.Options.BudgetNegotiation = scheduler.BudgetNegotiationTypeNone
					// }
					budgetNegotiation = scheduler.BudgetNegotiationTypeNone
				}

				// for larger fanouts, do whatever needed to negotiate
				if 	budgetNegotiation == scheduler.BudgetNegotiationTypeCDFBlock ||
				budgetNegotiation == scheduler.BudgetNegotiationTypeCDFNonBlock ||
					dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block || 
					dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
					
					if 	budgetNegotiation == scheduler.BudgetNegotiationTypeCDFNonBlock ||
						dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
						// for non-block negotiation, do not put into cache
						// so nothing to do here
					} else if budgetNegotiation == scheduler.BudgetNegotiationTypeCDFBlock ||
							  dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block {
					    // the cache is only used for blockable negotiation
						budgetnegotationCache = scheduler.NewBudgetNegotiationResponseCache()
						for _, neighbor := range eligibleNeighbors {
							budgetnegotationCache.Responses[neighbor.Key()] = &scheduler.BudgetNegotiationResponseCacheItem{
								Neighbor: neighbor,
								IsDone: false,
								RequestSentAt: time.Now(),
							}
						}
						tmpDispatchItem := dispatchItem.MinimumCopy()
						tmpDispatchItem.SetReportToForModule(string(kernel.AppModuleAggregator), j.Config.SelfNode.GetSDKNode(), nil)
						
						tmpDispatchItem.TTL = dispatchItem.TTL - 1

						tmpDispatchItem.Options = &scheduler.TaskDispatchingOptions{
							// BudgetNegotiation: budgetNegotiation,
							BudgetEstimationPercentilePoint: budgetEstimationPercentilePoint,
						}
						if dispatchItem.Options.CDFPoints > 0 {
							tmpDispatchItem.Options.CDFPoints = dispatchItem.Options.CDFPoints
						}
						if dispatchItem.Options.CDFStartPoint > 0 && dispatchItem.Options.CDFStartPoint <= 1 {
							tmpDispatchItem.Options.CDFStartPoint = dispatchItem.Options.CDFStartPoint
						}
						for _, neighbor := range eligibleNeighbors {
							go j.inquiryBudget(neighbor, tmpDispatchItem, budgetnegotationCache)
						}
						to_cache_neighbor_subtask = false
					}
					
				} else {
					targetPercentile := math.Pow(budgetEstimationPercentilePoint, 1.0/float64(len(eligibleNeighbors)))
					if dispatchItem.Options == nil {
						dispatchItem.Options = &scheduler.TaskDispatchingOptions{}
					}
					dispatchItem.Options.BudgetEstimationPercentilePoint = targetPercentile
					j.log.Printf("[budget negotiation] one-way negotiation by setting budget estimation percentile point to %v for %v eligible neighbors, where original percentile point is %v", targetPercentile, len(eligibleNeighbors), budgetEstimationPercentilePoint)
				}
			} 
			if to_cache_neighbor_subtask {
				for _, neighbor := range eligibleNeighbors {
					if neighbor.GetKey () != j.Config.SelfNode.GetKey() {
						dispatchItem.Task.SaveNeighborNode(neighbor)
					}
				}
			}

		}

		go j.CallbackOfNegotiation(budgetnegotationCache, dispatchItem)

	}
}

func (j *JADE) CallbackOfNegotiation(cache *scheduler.BudgetNegotiationResponseCache, dispatchItem *scheduler.TaskDispatchingItem) {
	
	dispatchItem.TTL--
	budgetNegotiation := scheduler.BudgetNegotiationTypeNone
	// if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiation != "" {
	// 	budgetNegotiation = dispatchItem.Options.BudgetNegotiation
	// }
	if dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
		budgetNegotiation = scheduler.BudgetNegotiationTypeCDFNonBlock
	} else if dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block {
		budgetNegotiation = scheduler.BudgetNegotiationTypeCDFBlock
	}

	if 	dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock ||
		budgetNegotiation == scheduler.BudgetNegotiationTypeCDFNonBlock {
		// for non-block negotiation, the budget inquiry process will happen when the local resources have been provisioned
		dispatchItem.SetBudgetForModule(string(kernel.AppModuleWorker), -1)
		dispatchItem.Options.BudgetNegotiationPhase = scheduler.BudgetNegotiationPhaseInquiry
	} else {
		// if cache is not nil, wait for cache done
		for {

			isDone := true
			if cache != nil {
				cache.Lock()
				for _, cacheItem := range cache.Responses {
					if !cacheItem.IsDone {
						isDone = false
						break
					}
				}
				cache.Unlock()
			}

			if isDone {
				break
			}

			time.Sleep(50000 * time.Nanosecond)
		}
		if cache != nil {
			j.log.Printf("[budget negotiation] all inquiries are done")
		}

		dispatchItem.InquiryDoneTimestamp = time.Now()

		if cache != nil {
			cache.Lock()

			// multiply CDFs
			var cdf_list []*histogram.CDF
			for _, cacheItem := range cache.Responses {
				if cacheItem.Response == nil || cacheItem.Response.CDF == nil {
					continue
				}
				// to be simpler in research, we do not reject neighbors regarding their CDFs
				// but if in business, we should
				cdf_list = append(cdf_list, cacheItem.Response.CDF)
				if cacheItem.Neighbor.GetKey () != j.Config.SelfNode.GetKey() {
					dispatchItem.Task.SaveNeighborNode(cacheItem.Neighbor)
				}
			}

			j.CalcGlobalBudget(cdf_list, dispatchItem)

			cache.Unlock()
			
		} else {
			// here is typically for non-negotiation
			dispatchItem.BudgetEstimationDoneTimestamp = time.Now()
			if dispatchItem.SLO != nil {
				provisionOverhead := float64(dispatchItem.BudgetEstimationDoneTimestamp.Sub(dispatchItem.ArriveTimestamp)*10 / time.Millisecond)/10
				dispatchItem.SLO.TailLatencyInMilliseconds -= provisionOverhead
			}
		}

		j.log.Printf("[budget negotiation] going to dispatch the task among all eligible clusters, there are %v neighbor subtasks", len(dispatchItem.Task.NeighborNodes))
	}

	j.evaluateAggregativeTasks(map[string]*scheduler.TaskDispatchingItem{
		dispatchItem.Task.GetKey(): dispatchItem,
	})

}

func (j *JADE) CalcGlobalBudget(cdf_list []*histogram.CDF, dispatchItem *scheduler.TaskDispatchingItem) {
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

		j.log.Printf("[budget negotiation] %v percentile tail latency of %v CDFs is %v",  budgetEstimationPercentilePoint*100, len(cdf_list), tail_latency)

		dispatchItem.BudgetEstimationDoneTimestamp = time.Now()

		negotiationOverhead = float64(dispatchItem.BudgetEstimationDoneTimestamp.Sub(dispatchItem.ArriveTimestamp) *10 / time.Millisecond ) /10

		if tail_latency < tailLatencySLO - negotiationOverhead {
			budget = tailLatencySLO - tail_latency - negotiationOverhead
			// budget = tailLatencySLO - tail_latency
		} else {
			budget = 0
		}

		dispatchItem.SetBudgetForModule(string(kernel.AppModuleWorker), budget)


		j.log.Printf("[budget negotiation] budget negotiation done in %v milliseconds, budget estimation done in %v milliseconds",
			dispatchItem.InquiryDoneTimestamp.Sub(dispatchItem.InquiryStartTimestamp) / time.Millisecond,
			dispatchItem.BudgetEstimationDoneTimestamp.Sub(dispatchItem.InquiryDoneTimestamp) / time.Millisecond,
		)

		j.log.Printf("[budget negotiation] tail latency SLO: %v, estimated budget: %v, deducted budget negotiation overhead: %v milliseconds", tailLatencySLO, budget, negotiationOverhead )

	} else {
		dispatchItem.BudgetEstimationDoneTimestamp = time.Now()
	}
}


func (j *JADE) inquiryBudget(neighbor *kernel.Node, sampleTask *scheduler.TaskDispatchingItem, cache *scheduler.BudgetNegotiationResponseCache) *scheduler.BudgetNegotiationResponse {
	payload := j.GeneratePayloadOfRequest(neighbor, sampleTask, nil, nil)

	j.log.Printf("[budget negotiation] inquirying eligible neighbor %v for budget on task %v ", neighbor, sampleTask)
	apiPath := "/$jade$/inquiryBudget"
	_, _, content, err := j.HTTPCommunicate("inquirying eligible neighbor", "POST", apiPath, neighbor, payload, 0, 10)
	if err != nil {
		j.log.Println("[budget negotiation] ERROR when inquirying eligible neighbor:", err.Error())
	} else {
		resInst :=  &struct{
			Payload *scheduler.BudgetNegotiationResponse `json:"payload,omitempty"`
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


	j.log.Printf("[budget negotiation] fetching eligible neighbors from registry node [%v]", j.Config.RegistryNode)
	if j.Config.RegistryNode.IsAddrEmpty() {
		j.log.Printf("[budget negotiation] ERROR: registry node is empty")

	}
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
