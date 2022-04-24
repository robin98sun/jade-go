package jadelet

import (
	// "time"
	// "sort"
	"encoding/json"
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
		j.log.Printf("got eligible neighbors: %v", eligibleNeighbors)
		j.eligibleNeighborCache.StoreEligibleNeighbors(dispatchItem.Task.GetKey(), eligibleNeighbors)
	}
}

func (j *JADE) fetchEligibleAutonomyServiceDomains(dispatchItem *scheduler.TaskDispatchingItem) []*kernel.Node {
	task := dispatchItem.Task
	if task == nil || task.Requirements == nil {
		return nil
	}
	payload := j.GeneratePayloadOfRequest(j.Config.RegistryNode, task.Requirements, nil, nil)

	j.log.Printf("fetching eligible neighbors from registry node [%v], which is %v empty", j.Config.RegistryNode, j.Config.RegistryNode.IsAddrEmpty())
	apiPath := "/$jade$/eligibleNeighbors"
	_, _, content, err := j.HTTPCommunicate("fetch eligible neighbors", "POST", apiPath, j.Config.RegistryNode, payload, 0, 10)
	if err != nil {
		j.log.Println("ERROR when fetching eligible neighbors:", err.Error())
	} else {
		resInst := []*kernel.Node{}
		err = json.Unmarshal(content, resInst)
		if err != nil {
			j.log.Println("ERROR of fetching eligible neighbors: can not decode response, ", err)
		} else {
			j.log.Println("response of fetching eligible neighbors:", resInst)
			return resInst
		}
	}
	return nil
}

