package jadelet

import (
	"github.com/ant0ine/go-json-rest/rest"
	"net/http"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jadesdk"
	"runtime"
)

// UpdateConfigurations to configure JADE at runtime
func (j *JADE) UpdateConfigurations(w rest.ResponseWriter, r *rest.Request) {
	j.log.Op.Println("updating configuration")
	c := kernel.NewConfiguration()
	err := r.DecodeJsonPayload(c)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	originalNodeKey := ""
	if !j.Config.SelfNode.IsAddrEmpty() {
		originalNodeKey = j.Config.SelfNode.Key()
	}
	j.Config = c

	newNodeKey := ""
	if !j.Config.SelfNode.IsAddrEmpty() {
		newNodeKey = j.Config.SelfNode.Key()
	}
	if originalNodeKey != newNodeKey && originalNodeKey != "" {
		j.log.Op.Printf("removing old capabilities for old nodekey[%v] while updating configuration", originalNodeKey)
		j.subnodeCapabilityCache.DeleteNode(originalNodeKey)
		j.neighborCapabilityCache.DeleteNode(originalNodeKey)
	}

	if newNodeKey != "" {
		j.log.Op.Printf("setting new capabilities for new nodekey[%v] while updating configuration", newNodeKey)
		if list, e := j.Config.Capabilities["public"]; e {
			j.subnodeCapabilityCache.Set(newNodeKey, list)
			j.neighborCapabilityCache.Set(newNodeKey, list)
		}
	}

	if j.Config.Options != nil && j.log != nil {
		j.log.Debug.Enabled = j.Config.Options.DebugLog
		j.log.Perf.Enabled = j.Config.Options.PerfLog
		j.log.Op.Enabled = j.Config.Options.OpLog 
	}
	w.WriteJson(c)
	// renew itself in upper node
	// go j.RegisterToUpperNode(0)
}

// AddCapability add a capability to self-node
func (j *JADE) AddCapability(w rest.ResponseWriter, r *rest.Request) {
	nc := &struct{
		Capability *jadesdk.Capability
		Type string
	}{}
	err := r.DecodeJsonPayload(nc)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	j.Config.AddOrUpdateCapability(nc.Type, nc.Capability)
	w.WriteJson(nc)
	// renew itself in upper node
	// go j.RegisterToUpperNode(0)
}

// DeleteCapability delete a capability of self-node
func (j *JADE) DeleteCapability(w rest.ResponseWriter, r *rest.Request) {
	nc := &struct{
		Capability *jadesdk.Capability
		Type string
	}{}
	err := r.DecodeJsonPayload(nc)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var deleted *jadesdk.Capability
	if nc.Capability != nil {
		deleted = j.Config.DeleteCapability(nc.Type, nc.Capability.Name)
	}
	w.WriteJson(deleted)
	// renew itself in upper node
	// go j.RegisterToUpperNode(0)
}

func (j *JADE) ClearTaskCacheAndStat(w rest.ResponseWriter, r *rest.Request) {
	tasksCleared := 0
	if j.TaskCache != nil {
		j.log.Op.Printf("clearing task cache")

		req := &struct{
			Seconds int `json:"seconds,omitempty"`
		}{}
		
		err := r.DecodeJsonPayload(req)

		if err != nil {
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		seconds := req.Seconds

		if seconds <= 0 {
			// perform GC on all app pods
			for _, pod := range j.PodCache.GetAllPods() {
				j.HTTPCommunicate(
					"GC on pod "+pod.GetKey(), "DELETE", "/$jade$/GC",
					pod.GetNodeRepresentation(j.Config.SelfNode.Protocol),
					nil,
					0, 10,
				)
			}
		}
		
		// Clear self cache
		tasksCleared = j.TaskCache.Clear(seconds)

		if seconds <=0 {
			// perform GC
			runtime.GC()
		}	
		j.log.Op.Printf("task cache is cleared")
	}
	j.DoneRequest(w, r, tasksCleared)
}

func (j *JADE) ClearPodCache(w rest.ResponseWriter, r *rest.Request) {
	if j.PodCache != nil {
		j.log.Op.Printf("clearing pod cache")
		j.PodCache.Clear()
		runtime.GC()
		j.log.Op.Printf("pod cache is cleared")
	}
	j.DoneRequest(w, r, "OK")
}

func (j *JADE) ClearPerfCache(w rest.ResponseWriter, r *rest.Request) {
	if j.PerfCache != nil {
		j.log.Op.Printf("clearing performance cache")
		j.PerfCache.Clear()
		runtime.GC()
		j.log.Op.Printf("performance cache is cleared")
	}
	j.DoneRequest(w, r, "OK")
}
