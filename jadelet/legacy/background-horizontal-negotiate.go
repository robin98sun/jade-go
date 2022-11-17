package jadelet

import (
	"time"
	"encoding/json"
<<<<<<< HEAD:jadelet/legacy/background-horizontal-negotiate.go
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/scheduler/histogram"
	"uta.edu/aces/scheduler/task"
=======
	// "uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	// "uta.edu/aces/jade-go/histogram"
	// "uta.edu/aces/jadesdk"
>>>>>>> refactoring:jadelet/background-horizontal-negotiate.go
	ds "uta.edu/aces/jadesdk/data_structure"
	"math"
)

func (j *JADE) evaluateCollaborativeTasks(tasklist map[string]*ds.TaskDispatchingItem) {
	for _, dispatchItem := range tasklist {
		if dispatchItem.Task == nil || dispatchItem.Task.Requirements == nil {
			continue
		}
		query := dispatchItem.Task.Requirements
		query_key := query.GetQueryKey()
		if query_key == "" {
			continue
		}

		eligibleNeighbors, _, _, _ := j.discoverNeighbors(dispatchItem)
		
		dispatchItem.InquiryStartTimestamp = time.Now()
		var budgetnegotationCache *task.BudgetNegotiationResponseCache
		to_cache_neighbor_subtask := true
		if len(eligibleNeighbors) > 0 {

			// data plane
			j.log.Debug.Printf("[budget negotiation] retrieved %v eligible neighbors from cache", len(eligibleNeighbors))
			// for some options, no need to negotiate budget

			if 	dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL ||
				dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_None ||
				dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_Block || 
				dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_NonBlock {

<<<<<<< HEAD:jadelet/legacy/background-horizontal-negotiate.go
				budgetNegotiation := task.BudgetNegotiationTypeNone
				// if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiation != "" {
				// 	budgetNegotiation = dispatchItem.Options.BudgetNegotiation
				// }
				if dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
					budgetNegotiation = task.BudgetNegotiationTypeCDFNonBlock
				} else if dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block {
					budgetNegotiation = task.BudgetNegotiationTypeCDFBlock
=======
				budgetNegotiation := ds.BudgetNegotiationTypeNone
				// if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiation != "" {
				// 	budgetNegotiation = dispatchItem.Options.BudgetNegotiation
				// }
				if dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_NonBlock {
					budgetNegotiation = ds.BudgetNegotiationTypeCDFNonBlock
				} else if dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_Block {
					budgetNegotiation = ds.BudgetNegotiationTypeCDFBlock
>>>>>>> refactoring:jadelet/background-horizontal-negotiate.go
				}

				budgetEstimationPercentilePoint := float64(0.95)
				if dispatchItem.Options != nil && dispatchItem.Options.BudgetEstimationPercentilePoint > 0 && dispatchItem.Options.BudgetEstimationPercentilePoint <= 1 {
					budgetEstimationPercentilePoint = dispatchItem.Options.BudgetEstimationPercentilePoint
				}

				// for single fanout, no need to negotiate
				if len(eligibleNeighbors) == 1 {
					dispatchItem.Task.QueuingMechanism = ds.TaskQueuingDDL_None
					// if dispatchItem.Options != nil  {
					// 	dispatchItem.Options.BudgetNegotiation = task.BudgetNegotiationTypeNone
					// }
<<<<<<< HEAD:jadelet/legacy/background-horizontal-negotiate.go
					budgetNegotiation = task.BudgetNegotiationTypeNone
				}

				// for larger fanouts, do whatever needed to negotiate
				if 	budgetNegotiation == task.BudgetNegotiationTypeCDFBlock ||
					budgetNegotiation == task.BudgetNegotiationTypeCDFNonBlock ||
					dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block || 
					dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
					
					if 	budgetNegotiation == task.BudgetNegotiationTypeCDFNonBlock ||
						dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
						// for non-block negotiation, do not put into cache
						// so nothing to do here
					} else if budgetNegotiation == task.BudgetNegotiationTypeCDFBlock ||
							  dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block {
=======
					budgetNegotiation = ds.BudgetNegotiationTypeNone
				}

				// for larger fanouts, do whatever needed to negotiate
				if 	budgetNegotiation == ds.BudgetNegotiationTypeCDFBlock ||
					budgetNegotiation == ds.BudgetNegotiationTypeCDFNonBlock ||
					dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_Block || 
					dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_NonBlock {
					
					if 	budgetNegotiation == ds.BudgetNegotiationTypeCDFNonBlock ||
						dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_NonBlock {
						// for non-block negotiation, do not put into cache
						// so nothing to do here
					} else if budgetNegotiation == ds.BudgetNegotiationTypeCDFBlock ||
							  dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_Block {
>>>>>>> refactoring:jadelet/background-horizontal-negotiate.go
					    // the cache is only used for blockable negotiation
						budgetnegotationCache = task.NewBudgetNegotiationResponseCache()
						for _, neighbor := range eligibleNeighbors {
							budgetnegotationCache.Responses[neighbor.Key()] = &task.BudgetNegotiationResponseCacheItem{
								Neighbor: neighbor,
								IsDone: false,
								RequestSentAt: time.Now(),
							}
						}
						tmpDispatchItem := dispatchItem.MinimumCopy()
						tmpDispatchItem.SetReportToForModule(string(ds.AppModuleAggregator), j.Config.SelfNode.GetSDKNode(), nil)
						
						tmpDispatchItem.TTL = dispatchItem.TTL - 1

<<<<<<< HEAD:jadelet/legacy/background-horizontal-negotiate.go
						tmpDispatchItem.Options = &task.TaskDispatchingOptions{
=======
						tmpDispatchItem.Options = &ds.TaskDispatchingOptions{
>>>>>>> refactoring:jadelet/background-horizontal-negotiate.go
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
<<<<<<< HEAD:jadelet/legacy/background-horizontal-negotiate.go
						dispatchItem.Options = &task.TaskDispatchingOptions{}
=======
						dispatchItem.Options = &ds.TaskDispatchingOptions{}
>>>>>>> refactoring:jadelet/background-horizontal-negotiate.go
					}
					dispatchItem.Options.BudgetEstimationPercentilePoint = targetPercentile
					j.log.Debug.Printf("[budget negotiation] one-way negotiation by setting budget estimation percentile point to %v for %v eligible neighbors, where original percentile point is %v", targetPercentile, len(eligibleNeighbors), budgetEstimationPercentilePoint)
				}
			} 
			if to_cache_neighbor_subtask {
				count_neighbors := 0
				for _, neighbor := range eligibleNeighbors {
					if neighbor.GetKey () != j.Config.SelfNode.GetKey() {
						dispatchItem.Task.SaveNeighborNode(neighbor)
						count_neighbors += 1
					}
				}
				j.log.Debug.Printf("[budget negotiation] there are %v real neighbors among the %v eligible neighbors", count_neighbors, len(eligibleNeighbors))
			}

		}

		go j.CallbackOfNegotiation(budgetnegotationCache, dispatchItem)

	}
}

<<<<<<< HEAD:jadelet/legacy/background-horizontal-negotiate.go
func (j *JADE) CallbackOfNegotiation(cache *task.BudgetNegotiationResponseCache, dispatchItem *ds.TaskDispatchingItem) {
	
	dispatchItem.TTL--
	budgetNegotiation := task.BudgetNegotiationTypeNone
	// if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiation != "" {
	// 	budgetNegotiation = dispatchItem.Options.BudgetNegotiation
	// }
	if dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock {
		budgetNegotiation = task.BudgetNegotiationTypeCDFNonBlock
	} else if dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_Block {
		budgetNegotiation = task.BudgetNegotiationTypeCDFBlock
	}

	if 	dispatchItem.Task.QueuingMechanism == kernel.TaskQueuingDDL_CDF_NonBlock ||
		budgetNegotiation == task.BudgetNegotiationTypeCDFNonBlock {
		// for non-block negotiation, the budget inquiry process will happen when the local resources have been provisioned
		dispatchItem.SetBudgetForModule(string(kernel.AppModuleWorker), -1)
		dispatchItem.Options.BudgetNegotiationPhase = task.BudgetNegotiationPhaseInquiry
=======
func (j *JADE) CallbackOfNegotiation(cache *scheduler.BudgetNegotiationResponseCache, dispatchItem *ds.TaskDispatchingItem) {
	
	dispatchItem.TTL--
	budgetNegotiation := ds.BudgetNegotiationTypeNone
	// if dispatchItem.Options != nil && dispatchItem.Options.BudgetNegotiation != "" {
	// 	budgetNegotiation = dispatchItem.Options.BudgetNegotiation
	// }
	if dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_NonBlock {
		budgetNegotiation = ds.BudgetNegotiationTypeCDFNonBlock
	} else if dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_Block {
		budgetNegotiation = ds.BudgetNegotiationTypeCDFBlock
	}

	if 	dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_NonBlock ||
		budgetNegotiation == ds.BudgetNegotiationTypeCDFNonBlock {
		// for non-block negotiation, the budget inquiry process will happen when the local resources have been provisioned
		dispatchItem.SetBudgetForModule(string(ds.AppModuleWorker), -1)
		dispatchItem.Options.BudgetNegotiationPhase = ds.BudgetNegotiationPhaseInquiry
>>>>>>> refactoring:jadelet/background-horizontal-negotiate.go
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
			j.log.Debug.Printf("[budget negotiation] all inquiries are done")
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
			
		// } else {
		}
			// here is typically for non-negotiation
		dispatchItem.BudgetEstimationDoneTimestamp = time.Now()
		if dispatchItem.SLO != nil && 
			( dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL ||
			  dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_None ||
			  dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_Block ||
			  dispatchItem.Task.QueuingMechanism == ds.TaskQueuingDDL_CDF_NonBlock){
			provisionOverhead := float64(dispatchItem.BudgetEstimationDoneTimestamp.Sub(dispatchItem.ArriveTimestamp)*10 / time.Millisecond)/10
			dispatchItem.SLO.TailLatencyInMilliseconds -= provisionOverhead
		}
		// }

		j.log.Debug.Printf("[budget negotiation] going to dispatch the task among all eligible clusters, there are %v neighbor subtasks", len(dispatchItem.Task.NeighborNodes))
	}

<<<<<<< HEAD:jadelet/legacy/background-horizontal-negotiate.go
	j.evaluateAggregativeTasks(map[string]*task.TaskDispatchingItem{
=======
	j.evaluateAggregativeTasks(map[string]*ds.TaskDispatchingItem{
>>>>>>> refactoring:jadelet/background-horizontal-negotiate.go
		dispatchItem.Task.GetKey(): dispatchItem,
	})

}

func (j *JADE) CalcGlobalBudget(cdf_list []*histogram.CDF, dispatchItem *ds.TaskDispatchingItem) {
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

		j.log.Debug.Printf("[budget negotiation] %v percentile tail latency of %v CDFs is %v",  budgetEstimationPercentilePoint*100, len(cdf_list), tail_latency)

		dispatchItem.BudgetEstimationDoneTimestamp = time.Now()

		negotiationOverhead = float64(dispatchItem.BudgetEstimationDoneTimestamp.Sub(dispatchItem.ArriveTimestamp) *10 / time.Millisecond ) /10

		if tail_latency < tailLatencySLO - negotiationOverhead {
			budget = tailLatencySLO - tail_latency - negotiationOverhead
			// budget = tailLatencySLO - tail_latency
		} else {
			budget = 0
		}

		dispatchItem.SetBudgetForModule(string(ds.AppModuleWorker), budget)

		j.TaskCache.SetUnloadedTailLatencyAndBudgetForTask(dispatchItem.Task.GetKey(), tail_latency, budget)


		j.log.Debug.Printf("[budget negotiation] budget negotiation done in %v milliseconds, budget estimation done in %v milliseconds",
			dispatchItem.InquiryDoneTimestamp.Sub(dispatchItem.InquiryStartTimestamp) / time.Millisecond,
			dispatchItem.BudgetEstimationDoneTimestamp.Sub(dispatchItem.InquiryDoneTimestamp) / time.Millisecond,
		)

		j.log.Debug.Printf("[budget negotiation] tail latency SLO: %v, estimated budget: %v, deducted budget negotiation overhead: %v milliseconds", tailLatencySLO, budget, negotiationOverhead )

	} else {
		dispatchItem.BudgetEstimationDoneTimestamp = time.Now()
	}
}


<<<<<<< HEAD:jadelet/legacy/background-horizontal-negotiate.go
func (j *JADE) inquiryBudget(neighbor *ds.Node, sampleTask *ds.TaskDispatchingItem, cache *task.BudgetNegotiationResponseCache) *task.BudgetNegotiationResponse {
=======
func (j *JADE) inquiryBudget(neighbor *ds.Node, sampleTask *ds.TaskDispatchingItem, cache *scheduler.BudgetNegotiationResponseCache) *scheduler.BudgetNegotiationResponse {
>>>>>>> refactoring:jadelet/background-horizontal-negotiate.go
	payload := j.GeneratePayloadOfRequest(neighbor, sampleTask, nil, nil)

	j.log.Debug.Printf("[budget negotiation] inquirying eligible neighbor %v for budget on task %v ", neighbor, sampleTask)
	apiPath := "/$jade$/inquiryBudget"
	_, _, content, err := j.HTTPCommunicate("inquirying eligible neighbor", "POST", apiPath, neighbor, payload, 0, 10)
	if err != nil {
		j.log.Debug.Println("[budget negotiation] ERROR when inquirying eligible neighbor:", err.Error())
	} else {
		resInst :=  &struct{
			Payload *task.BudgetNegotiationResponse `json:"payload,omitempty"`
		}{}
		err = json.Unmarshal(content, resInst)
		if err != nil {
			j.log.Debug.Println("[budget negotiation] ERROR of inquirying eligible neighbor: can not decode response, ", err)
		} else {
			j.log.Debug.Println("[budget negotiation] response of inquirying eligible neighbor:", resInst.Payload)

			cache.SetResponse(neighbor, resInst.Payload)
			return resInst.Payload
		}
	}
	cache.SetResponse(neighbor, nil)
	return nil
}

<<<<<<< HEAD:jadelet/legacy/background-horizontal-negotiate.go
func (j *JADE) fetchEligibleAutonomyServiceDomains(query *ds.Requirements) []*ds.Node {
=======
func (j *JADE) fetchEligibleAutonomyServiceDomains(query *ds.Requirements) *InqueryNeighborResponse {
>>>>>>> refactoring:jadelet/background-horizontal-negotiate.go
	payload := j.GeneratePayloadOfRequest(j.Config.RegistryNode, query, nil, nil)


	j.log.Debug.Printf("[control plane] fetching eligible neighbors from registry node [%v]", j.Config.RegistryNode)
	if j.Config.RegistryNode.IsAddrEmpty() {
		j.log.Debug.Printf("[control plane] ERROR: registry node is empty")

	}
	apiPath := "/$jade$/eligibleNeighbors"
	_, _, content, err := j.HTTPCommunicate("fetch eligible neighbors", "POST", apiPath, j.Config.RegistryNode, payload, 0, 10)
	if err != nil {
		j.log.Debug.Println("[control plane] ERROR when fetching eligible neighbors:", err.Error())
	} else {
		resInst :=  &struct{
			Payload *InqueryNeighborResponse `json:"payload,omitempty"`
		}{}
		packageSize := len(content)
		err = json.Unmarshal(content, resInst)
		if err != nil {
			j.log.Debug.Println("[control plane] ERROR of fetching eligible neighbors: can not decode response, ", err)
		} else {
			j.log.Debug.Printf("[control plane] response of fetching eligible neighbors, package size: %v, nodes %v", packageSize, len(resInst.Payload.Nodes))
			resInst.Payload.PackageSize = packageSize
			return resInst.Payload
		}
	}
	return nil
}
