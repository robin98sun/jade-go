package jadelet

import (
	// "encoding/json"
	// "github.com/ant0ine/go-json-rest/rest"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jade-go/kernel"
	"time"
	"sync"
)


func (j *JADE) discoverNeighbors(dispatchItem *scheduler.TaskDispatchingItem) ([]*kernel.Node, int, float64) {
	dur := float64(0)
	packageSize := 0
	query := dispatchItem.Task.Requirements
	query_key := query.GetQueryKey()
	if query_key == "" {
		return nil, packageSize, dur
	}

	var eligibleNeighbors []*kernel.Node 
	overwriteCache := false

	if dispatchItem.Options!=nil && dispatchItem.Options.ControlPlaneOptions!=nil && dispatchItem.Options.ControlPlaneOptions.OverwriteCache {
		overwriteCache = true
	}

	if !overwriteCache {
		start_time := time.Now()
		eligibleNeighbors = j.eligibleNeighborCache.GetEligibleNeighbors(query_key)
		dur = float64(time.Now().Sub(start_time)) / float64(time.Millisecond)
	}

	if len(eligibleNeighbors) == 0 {
		res := j.fetchEligibleAutonomyServiceDomains(query)
		eligibleNeighbors = res.Nodes
		dur = res.Duration
		packageSize = res.PackageSize
		j.log.Debug.Printf("[control plane] got %v eligible neighbors from registry", len(eligibleNeighbors))
		if eligibleNeighbors == nil {
			eligibleNeighbors = []*kernel.Node{}
		}
		j.eligibleNeighborCache.StoreEligibleNeighbors(query_key, eligibleNeighbors)
	} else {
		j.log.Debug.Printf("[control plane] got %v eligible neighbors from cache", len(eligibleNeighbors))
	}

	return eligibleNeighbors, packageSize, dur
}

func (j *JADE) processControlPlaneTask(dispatchItem *scheduler.TaskDispatchingItem) (int, float64, float64, float64, int) {

	start_time := time.Now()
	discovery_time := float64(0)
	matching_time := float64(0)
	neighborCount := 0
	packageSize := 0
	if dispatchItem.TTL > 0 {
		eligibleNeighbors, ps, dur := j.discoverNeighbors(dispatchItem)
		matching_time = dur
		packageSize = ps
		discovery_time = float64(time.Now().Sub(start_time)) / float64(time.Millisecond)
		if len(eligibleNeighbors) == 0 {
			return neighborCount, discovery_time, discovery_time, matching_time, packageSize
		}
		neighborCount = len(eligibleNeighbors)
		dispatchItem.TTL -= 1
		inParallel := false
		if dispatchItem.Options != nil && dispatchItem.Options.ControlPlaneOptions != nil {
			inParallel = dispatchItem.Options.ControlPlaneOptions.InParallel
		}

		if inParallel {
			cache := &struct {
				mutex *sync.Mutex
				returnlist []bool
			}{
				mutex: &sync.Mutex{},
				returnlist: []bool{},
			}
			routine := func(node *kernel.Node) {
				j.dispatchNeighborTask(node, dispatchItem)
				cache.mutex.Lock()
				cache.returnlist = append(cache.returnlist, true)
				j.log.Op.Printf("[control plane][parallel negotiation] %v nodes done", len(cache.returnlist))
				cache.mutex.Unlock()
			}
			for _, node := range eligibleNeighbors {
				go routine(node)
			}
			for {
				cache.mutex.Lock()
				if len(cache.returnlist) == len(eligibleNeighbors) {
					cache.mutex.Unlock()
					break
				} else {
					j.log.Op.Printf("[control plane][parallel negotiation] still waiting for %v nodes", len(eligibleNeighbors)-len(cache.returnlist))
				}
				cache.mutex.Unlock()
			}
		} else {
			for _, node := range eligibleNeighbors {
				j.dispatchNeighborTask(node, dispatchItem)
			}
		}

	} else {
		if dispatchItem.Options != nil && dispatchItem.Options.ControlPlaneOptions != nil {
			if dispatchItem.Options.ControlPlaneOptions.WaitMillisecondsBeforeAnswer > 0 {
				time.Sleep(time.Duration(dispatchItem.Options.ControlPlaneOptions.WaitMillisecondsBeforeAnswer) * time.Millisecond)
			}
		}

	}
	return neighborCount, float64(time.Now().Sub(start_time)) / float64(time.Millisecond), discovery_time, matching_time, packageSize

}