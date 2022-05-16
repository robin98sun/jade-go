package jadelet

import (
	// "encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jade-go/histogram"
	"encoding/json"
)

func (j *JADE) RegisterNeighbor(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	_, payload, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}

	j.registerNode(JadeNodeTypeNeighbor, payload)
	
	// finish the request
	j.DoneRequest(w, r, nil)
}

func (j *JADE) ListNeighbors(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	content, _, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}

	reqInst := &struct {
		Payload *kernel.Requirements
	}{}
	err = json.Unmarshal(content, reqInst)

	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode requirements of listing eligible neighbors: "+err.Error())
		j.log.Println("[registry] ERROR of decoding content of requirements:", err.Error())
		return
	}

	requirements := reqInst.Payload

	nodekeys := j.selectAvaiableNodes(JadeNodeTypeNeighbor, requirements)

	var nodes []*kernel.Node
	if len(nodekeys) > 0 {
		j.registryMutex.Lock()
		defer j.registryMutex.Unlock()

		for _, nodeKey := range nodekeys {
			if _ , e := j.Neighbors[nodeKey]; e {
				nodes = append(nodes, j.Neighbors[nodeKey])
			} else if nodeKey == j.Config.SelfNode.Key() {
				nodes = append(nodes, j.Config.SelfNode)
			}
			// j.log.Printf("got eligible neighbor [%v]: %v", nodeKey, nodes[len(nodes)-1])
		}
	}
	j.log.Printf("[fetch neighbors] selected %v eligible neighbors", len(nodes))
	// finish the request
	j.DoneRequest(w, r, nodes)
}

func (j *JADE) CollectCDF(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	content, _, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}

	reqInst := &struct {
		Payload *scheduler.BudgetNegotiationResponse
	}{}
	err = json.Unmarshal(content, reqInst)
	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode CDF: "+err.Error())
		j.log.Println("[collect CDF] ERROR of decoding CDF:", err.Error())
		return
	}

	response := reqInst.Payload

	cache := j.TaskCache.GetBudgetNegotiationCache(response.TaskKey)
	if cache != nil {
		cache.SetResponse(response.Node, response)
		j.DoneRequest(w, r, "OK")
	} else {
		j.log.Printf("[collect CDF] ERROR: Budget Negotiatin Cache does not exist for task: %v", response.TaskKey)
		j.PeacefulFatalRequest(w, r, "cache does not exist")
	}
}

func (j *JADE) NeighborInquiry(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	content, _, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}

	reqInst := &struct {
		Payload *scheduler.TaskDispatchingItem
	}{}
	err = json.Unmarshal(content, reqInst)

	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode requirements of listing eligible neighbors: "+err.Error())
		j.log.Println("[inquiry] ERROR of decoding content of requirements:", err.Error())
		return
	}

	dispatchItem := reqInst.Payload

	if dispatchItem == nil || dispatchItem.Task == nil || dispatchItem.Task.Requirements == nil || dispatchItem.Task.Application == nil {
		j.PeacefulFatalRequest(w, r, "invalid task")
		j.log.Println("[inquiry] invalid incoming task")
		return
	}

	availableNodes := j.selectAvaiableNodes(JadeNodeTypeSubnode, dispatchItem.Task.Requirements)

	// response := &scheduler.BudgetNegotiationResponse{
	// 	AvailableNodes: int64(0),
	// }

	// var histogram_list []*histogram.Histogram
	pods := []*kernel.Pod{}
	if len(availableNodes) > 0 {
		// for non-block negotiation, schedule the job immediately

		// 
		for _, nodekey := range availableNodes {
			j.log.Printf("[inquiry] [jade version: %v] searching pod for application %v on node %v",
				j.Config.Version,
				dispatchItem.Task.Application.Key(),
				nodekey,
			)
			workerPod := j.PodCache.GetPodForApplication(nodekey, dispatchItem.Task.Application, string(kernel.AppModuleWorker), nil )
			if workerPod == nil {
				j.log.Printf("[inquiry]ERROR: NO worker pod for application %v on node %v",
					dispatchItem.Task.Application.Key(),
					nodekey,
				)
				continue
			}
			j.log.Printf("[inquiry] selected one pod [%v] for application %v on node %v",
				workerPod.GetKey(),
				dispatchItem.Task.Application.Key(),
				nodekey,
			)
			pods = append(pods, workerPod)

			
		}
		
	}
	response := j.MultiplyCDFs(pods, dispatchItem)

	// finish the request
	j.DoneRequest(w, r, response)
}


func (j *JADE) MultiplyCDFs(pods []*kernel.Pod, dispatchItem *scheduler.TaskDispatchingItem) *scheduler.BudgetNegotiationResponse {
	response := &scheduler.BudgetNegotiationResponse{
		AvailableNodes: int64(len(pods)),
		TaskKey: dispatchItem.Task.GetKey(),
		Node: j.Config.SelfNode.MiniNode(),
	}
	var histogram_list []*histogram.Histogram
	for _, workerPod := range pods {
		podQueue := j.PodCache.GetPodQueue(workerPod)
		if podQueue == nil {
			j.log.Printf("[inquiry]ERROR: the queue of pod [%v] for application %v is nil",
				workerPod.GetKey(),
				dispatchItem.Task.Application.Key(),
			)
			continue
		}
		j.log.Printf("[inquiry] got the queue of pod [%v] for application %v",
			workerPod.GetKey(),
			dispatchItem.Task.Application.Key(),
		)
		histogram_list = append(histogram_list, podQueue.HistogramServiceTime)
	}
	if len(histogram_list) > 0 {
		response.AvailableNodes = int64(len(histogram_list))
		count := 20
		if dispatchItem.Options != nil && dispatchItem.Options.CDFPoints > 0 {
			count = dispatchItem.Options.CDFPoints
		}
		response.CDF = histogram.NewCDF(count+1)
		response.CDF.StartPoint = float64(0.99) 
		if dispatchItem.Options != nil && dispatchItem.Options.CDFStartPoint > 0 && dispatchItem.Options.CDFStartPoint <= 1 {
			response.CDF.StartPoint = dispatchItem.Options.CDFStartPoint
		}
		response.CDF.Increment = (1-response.CDF.StartPoint)/float64(count)
		for i:=0; i<=count; i++ {
			percentile := response.CDF.StartPoint + float64(i) * response.CDF.Increment
			if i == count {
				percentile = float64(1)
			}
			latency := histogram.CalcPercentileOfProduct(percentile, histogram_list, false)
			response.CDF.Points[i] = &histogram.CDFPoint{
				Percentile: percentile,
				Value: latency,
			}
		}
	}
	j.log.Printf("[inquiry] selected %v histograms for application %v on %v nodes, response: %v",
		len(histogram_list), 
		dispatchItem.Task.Application.Key(),
		len(pods),
		response,
	)

	return response
}

func (j *JADE) NeighborGossip(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	_, _, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}
	
	// finish the request
	j.DoneRequest(w, r, nil)
}
