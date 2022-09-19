package jadelet

import (
	// "encoding/json"
	// "github.com/ant0ine/go-json-rest/rest"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jade-go/kernel"
	"time"
	"sync"
	"math"
)


func (j *JADE) discoverNeighbors(dispatchItem *scheduler.TaskDispatchingItem) ([]*kernel.Node, int, float64, float64) {
	matching := float64(0)
	populating := float64(0)
	packageSize := 0
	query := dispatchItem.Task.Requirements
	query_key := query.GetQueryKey()
	if query_key == "" {
		return nil, packageSize, matching, populating
	}

	var eligibleNeighbors []*kernel.Node 
	overwriteCache := false

	if dispatchItem.Options!=nil && dispatchItem.Options.ControlPlaneOptions!=nil {
		overwriteCache = dispatchItem.Options.ControlPlaneOptions.OverwriteCache
	}

	if !overwriteCache {
		start_time := time.Now()
		eligibleNeighbors = j.eligibleNeighborCache.GetEligibleNeighbors(query_key)
		matching = float64(time.Now().Sub(start_time)) / float64(time.Millisecond)
	} else {
		j.log.Debug.Printf("[control plane] overwriting neighbor cache for query: %v", query_key)
	}

	if len(eligibleNeighbors) == 0 {
		res := j.fetchEligibleAutonomyServiceDomains(query)
		eligibleNeighbors = res.Nodes
		populating = res.Duration - res.Matching
		matching = res.Matching

		packageSize = res.PackageSize
		j.log.Debug.Printf("[control plane] got %v eligible neighbors from registry", len(eligibleNeighbors))
		if eligibleNeighbors == nil {
			eligibleNeighbors = []*kernel.Node{}
		}
		j.eligibleNeighborCache.StoreEligibleNeighbors(query_key, eligibleNeighbors)
	} else {
		j.log.Debug.Printf("[control plane] got %v eligible neighbors from cache", len(eligibleNeighbors))
	}

	return eligibleNeighbors, packageSize, matching, populating
}

func (j *JADE) processControlPlaneTask(dispatchItem *scheduler.TaskDispatchingItem) (int, float64, float64, float64, float64, int, int) {

	start_time := time.Now()
	discovery_time := float64(0)
	matching_time := float64(0)
	populating_time := float64(0)
	neighborCount := 0
	packageSize := 0
	struggling_nodes := 0
	if dispatchItem.TTL > 0 {
		eligibleNeighbors, ps, t1, t2 := j.discoverNeighbors(dispatchItem)
		matching_time = t1
		populating_time = t2
		packageSize = ps
		discovery_time = float64(time.Now().Sub(start_time)) / float64(time.Millisecond)
		if len(eligibleNeighbors) == 0 {
			return neighborCount, discovery_time, discovery_time, matching_time, populating_time, packageSize, struggling_nodes
		}
		neighborCount = len(eligibleNeighbors)
		dispatchItem.TTL -= 1
		inParallel := false
		doNotDispatch := false
		if dispatchItem.Options != nil && dispatchItem.Options.ControlPlaneOptions != nil {
			inParallel = dispatchItem.Options.ControlPlaneOptions.InParallel
			doNotDispatch = dispatchItem.Options.ControlPlaneOptions.DoNotDispatch
		}

		if inParallel {
			cache := &struct {
				mutex *sync.Mutex
				returnlist map[string]bool
			}{
				mutex: &sync.Mutex{},
				returnlist: map[string]bool{},
			}
			routine := func(node *kernel.Node, i int) {
				time.Sleep(time.Duration(500+i*100)*time.Microsecond)
				j.log.Op.Printf("[control plane][parallel negotiation] dispatching to No.%v node", i)
				if ! doNotDispatch {
					j.dispatchNeighborTask(node, dispatchItem)
				} else {
					j.log.Op.Printf("[control plane][parallel negotiation] did not dispatch task to No.%v node due to emulation", i)
				}
				cache.mutex.Lock()
				cache.returnlist[node.Key()]=true
				cache.mutex.Unlock()
			}
			for i, node := range eligibleNeighbors {
				go routine(node, i)
			}

			iteration := 0
			for {
				time.Sleep(time.Duration(500)*time.Microsecond)
				count_waiting_nodes := 0
				cache.mutex.Lock()
				struggling_nodes = len(eligibleNeighbors)-len(cache.returnlist)
				if len(cache.returnlist) == len(eligibleNeighbors) {
					j.log.Op.Printf("[control plane][parallel negotiation] %v nodes all done", len(eligibleNeighbors))
					cache.mutex.Unlock()
					break
				} else if iteration % 1000 == 0 {
					j.log.Op.Printf("[control plane][parallel negotiation] still waiting for %v nodes", struggling_nodes)
				}
				for _, node := range eligibleNeighbors {
					if _, e := cache.returnlist[node.Key()]; !e {
						if iteration % 1000 == 0 {
							j.log.Op.Printf("[control plane][parallel negotiation] waiting for node[%v], hostname: %v, port: %v", 
								node.Key(), node.Hostname, node.Port,
							)
						}
						count_waiting_nodes += 1
					}
				}
				cache.mutex.Unlock()
				if count_waiting_nodes == 0 {
					j.log.Op.Printf("[control plane][parallel negotiation] stop waiting for %v nodes among %v due to emulation", 
						struggling_nodes, len(eligibleNeighbors),
					)
					break
				}
				iteration += 1
				if iteration > 40000 && struggling_nodes < len(eligibleNeighbors) / 10 || iteration > 80000 {
					j.log.Op.Printf("[control plane][parallel negotiation] stop waiting for %v nodes among %v after %v seconds", struggling_nodes, len(eligibleNeighbors), math.Round(float64(iteration)*0.5/100)/10,
					)
					break
				}
			}
		} else {
			for _, node := range eligibleNeighbors {
				if ! doNotDispatch {
					j.dispatchNeighborTask(node, dispatchItem)
				}
			}
		}

	} else {
		if dispatchItem.Options != nil && dispatchItem.Options.ControlPlaneOptions != nil {
			if dispatchItem.Options.ControlPlaneOptions.WaitMillisecondsBeforeAnswer > 0 {
				time.Sleep(time.Duration(dispatchItem.Options.ControlPlaneOptions.WaitMillisecondsBeforeAnswer) * time.Millisecond)
			}
		}

	}
	return neighborCount, float64(time.Now().Sub(start_time)) / float64(time.Millisecond), discovery_time, matching_time, populating_time, packageSize, struggling_nodes

}