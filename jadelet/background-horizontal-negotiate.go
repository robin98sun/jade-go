package jadelet

import (
	// "time"
	// "sort"
	"encoding/json"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler"
	// "uta.edu/aces/jade-go/histogram"
	// "uta.edu/aces/jadesdk"
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
		}

	}
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
			j.log.Println("response of fetching eligible neighbors:", resInst)
			return resInst.Payload
		}
	}
	return nil
}

