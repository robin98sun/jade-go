package jadelet

import (
	// "encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	ds "uta.edu/aces/jadesdk/data_structure"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jade-go/histogram"
	"encoding/json"
	"time"
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

type InqueryNeighborResponse struct {
	Populating float64 `json:"populating,omitempty"`
	Matching float64 `json:"matching,omitempty"`
	PackageSize int `json:packageSize,omitempty"`
	Nodes []*ds.Node `json:"nodes,omitempty"`
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
		Payload *ds.Requirements
	}{}
	err = json.Unmarshal(content, reqInst)

	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode requirements of listing eligible neighbors: "+err.Error())
		j.log.Op.Println("[registry] ERROR of decoding content of requirements:", err.Error())
		return
	}

	requirements := reqInst.Payload

	start_time := time.Now()
	nodekeys := j.selectAvaiableNodes(JadeNodeTypeNeighbor, requirements)
	matching := float64(time.Now().Sub(start_time)) / float64(time.Millisecond)

	start_time = time.Now()
	var nodes []*ds.Node
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
	populating := float64(time.Now().Sub(start_time)) / float64(time.Millisecond)
	j.log.Op.Printf("[fetch neighbors] selected %v eligible neighbors", len(nodes))
	// finish the request
	res := &InqueryNeighborResponse{
		Populating: populating,
		Matching: matching,
		Nodes: nodes,
	}
	j.DoneRequest(w, r, res)
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
		Payload *task.BudgetNegotiationResponse
	}{}
	err = json.Unmarshal(content, reqInst)
	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode CDF: "+err.Error())
		j.log.Op.Println("[collect CDF] ERROR of decoding CDF:", err.Error())
		return
	}

	response := reqInst.Payload

	cache := j.TaskCache.GetBudgetNegotiationCache(response.TaskKey)
	if cache != nil {
		cache.SetResponse(response.Node, response)
		j.DoneRequest(w, r, "OK")
	} else {
		j.log.Op.Printf("[collect CDF] ERROR: Budget Negotiatin Cache does not exist for task: %v", response.TaskKey)
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
		Payload *ds.TaskDispatchingItem
	}{}
	err = json.Unmarshal(content, reqInst)

	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode requirements of listing eligible neighbors: "+err.Error())
		j.log.Op.Println("[inquiry] ERROR of decoding content of requirements:", err.Error())
		return
	}

	dispatchItem := reqInst.Payload

	if dispatchItem == nil || dispatchItem.Task == nil || dispatchItem.Task.Requirements == nil || dispatchItem.Task.Application == nil {
		j.PeacefulFatalRequest(w, r, "invalid task")
		j.log.Op.Println("[inquiry] invalid incoming task")
		return
	}

	availableNodes := j.selectAvaiableNodes(JadeNodeTypeSubnode, dispatchItem.Task.Requirements)

	// response := &task.BudgetNegotiationResponse{
	// 	AvailableNodes: int64(0),
	// }

	// var histogram_list []*histogram.Histogram
	var histogram_list []*histogram.Histogram
	if len(availableNodes) > 0 {
		// for non-block negotiation, schedule the job immediately

		// 
		for _, nodekey := range availableNodes {
			j.log.Op.Printf("[inquiry] [jade version: %v] searching pod for application %v on node %v",
				j.Config.Version,
				dispatchItem.Task.Application.Key(),
				nodekey,
			)
			workerScheduler := j.PodCache.GetNodeSchedulerForModule(nodekey, dispatchItem.Task.Application.Key(), string(ds.AppModuleWorker), nil )
			if workerScheduler == nil || workerScheduler.IsEmpty() {
				j.log.Op.Printf("[inquiry]ERROR: NO worker pod for application %v on node %v",
					dispatchItem.Task.Application.Key(),
					nodekey,
				)
				continue
			}
			j.log.Op.Printf("[inquiry] selected one node [%v] for application %v on node %v",
				workerScheduler.NodeKey,
				dispatchItem.Task.Application.Key(),
				nodekey,
			)
			histogram_list = append(histogram_list, workerScheduler.Queue.HistogramServiceTime)
		}
		
	}
	response := j.MultiplyCDFs(histogram_list, dispatchItem)

	// finish the request
	j.DoneRequest(w, r, response)
}


func (j *JADE) MultiplyCDFs(histogram_list []*histogram.Histogram, dispatchItem *ds.TaskDispatchingItem) *scheduler.BudgetNegotiationResponse {
	response := &scheduler.BudgetNegotiationResponse{
		AvailableNodes: int64(len(histogram_list)),
		TaskKey: dispatchItem.Task.GetKey(),
		Node: j.Config.SelfNode.MiniNode(),
	}
	// for _, workerPodKey := range pods {
	// 	podQueue := j.PodCache.GetSTQueue(workerPodKey)
	// 	if podQueue == nil {
	// 		j.log.Op.Printf("[inquiry] ERROR: the queue of pod [%v] for application %v is nil",
	// 			workerPodKey,
	// 			dispatchItem.Task.Application.Key(),
	// 		)
	// 		continue
	// 	}
	// 	j.log.Op.Printf("[inquiry] got the queue of pod [%v] for application %v",
	// 		workerPodKey,
	// 		dispatchItem.Task.Application.Key(),
	// 	)
	// 	histogram_list = append(histogram_list, podQueue.HistogramServiceTime)
	// }
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
	j.log.Op.Printf("[inquiry] selected %v histograms for application %v on %v nodes, response: %v",
		len(histogram_list), 
		dispatchItem.Task.Application.Key(),
		len(histogram_list),
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
