package jadelet

import (
	// "time"
	// "sort"
	// "encoding/json"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler"
	// "uta.edu/aces/jade-go/histogram"
)

func (j *JADE) evaluateCollaborativeTasks(tasklist map[string]*scheduler.TaskDispatchingItem) {
	for _, dispatchItem := range tasklist {
		if dispatchItem.Task == nil || dispatchItem.Task.Requirements == nil {
			continue
		}
		eligibleNeighbors := j.fetchEligibleAutonomyServiceDomains(dispatchItem)
		j.eligibleNeighborCache.StoreEligibleNeighbors(dispatchItem.Task.GetKey(), eligibleNeighbors)
	}
}

func (j *JADE) fetchEligibleAutonomyServiceDomains(dispatchItem *scheduler.TaskDispatchingItem) []*kernel.Node {
	task := dispatchItem.Task
	if task == nil || task.Requirements == nil {
		return nil
	}
	payload := j.GeneratePayloadOfRequest(j.Config.RegistryNode, task.Requirements, nil, nil)

	apiPath := "/$jade$/list"
	res, _, err := j.HTTPCommunicate("fetch eligible neighbors", "POST", apiPath, j.Config.RegistryNode, payload, 0, 10)
	if err != nil {
		j.log.Println("ERROR when fetching eligible neighbors:", err.Error())
	} else {
		// reqInst := &struct {
		// 	Payload []*kernel.Node `json:"payload,omitempty"`
		// }{}
		// err = json.Unmarshal(res, reqInst)
		// if err != nil {
		// 	j.log.Println("ERROR: can not decode eligible neighbors:", err.Error())
		// } else if reqInst == nil || reqInst.Payload == nil || len(reqInst.Payload) == 0 {
		// 	j.log.Println("received empty response for eligible neighbors")
		// } else {
		// 	// do validation if needed
		// 	return reqInst.Payload
		// }
		j.log.Println("response of fetching eligible neighbors:", res)

		if node_list, ok := res.([]*kernel.Node); ok {
			return node_list
		} else {
			j.log.Println("ERROR: can not decode response while fetching eligible neighbors:", ok)
		}
	}
	return nil
}

