package jadelet

import (
	"github.com/ant0ine/go-json-rest/rest"
	"net/http"
	rm "uta.edu/aces/jade-go/resource_manager"
	ds "uta.edu/aces/jadesdk/data_structure"
	"runtime"
	"encoding/json"
)

// UpdateConfigurations to configure JADE at runtime
func (j *JADE) UpdateConfigurations(w rest.ResponseWriter, r *rest.Request) {
	j.log.Op.Println("updating configuration")
	c := ds.NewConfiguration()
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
		Capability *ds.Capability
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
		Capability *ds.Capability
		Type string
	}{}
	err := r.DecodeJsonPayload(nc)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var deleted *ds.Capability
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


func (j *JADE) CleanAndResetQueues(w rest.ResponseWriter, r *rest.Request) {
	if j.PodCache != nil {
		j.log.Op.Printf("cleaning and reseting all queues")
		j.PodCache.CleanAndResetQueues()
		j.log.Op.Printf("all queues are cleaned up and well reset")
	}
	j.DoneRequest(w, r, "OK")
}

func (j *JADE) ShowPerfCache(w rest.ResponseWriter, r *rest.Request) {
	if j.PerfCache != nil {
		j.log.Op.Printf("getting perf cache")
		res, _ := json.MarshalIndent(j.PerfCache, "", " ")
		j.DoneRequest(w, r, res)
	} else {
		j.DoneRequest(w, r, "perf cache is nil")
	}
}

func (j *JADE) StartPerfEventListener(w rest.ResponseWriter, r *rest.Request) {
	
	j.PerfCache.StartPerfEventListener()
	j.DoneRequest(w, r, "OK")
}

func (j *JADE) StopPerfEventListener(w rest.ResponseWriter, r *rest.Request) {
	j.PerfCache.StopPerfEventListener()
	j.DoneRequest(w, r, "OK")
}

func (j *JADE) SetControlLoopParameters(w rest.ResponseWriter, r *rest.Request) {
	if j.PerfCache != nil {

		j.log.Op.Printf("updating perf cache parameters")
		params := &struct{
			Parameters *rm.ControlLoopParameters
			Type string
		}{}
		err := r.DecodeJsonPayload(params)
		if err != nil {
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		j.ControlLoop.Parameters = params.Parameters
		if params != nil && params.Parameters != nil {
			j.PerfCache.SetIterationTimeScaleInMilliseconds(params.Parameters.IterationTimeScaleInMilliseconds)
			j.PerfCache.SetMaximumTaskAmount(params.Parameters.MaximumTaskAmount)
			j.PerfCache.SetHistoryTimeWindowSize(params.Parameters.HistoryTimeWindowSize)
			j.ControlLoop.SetIterationTimeScaleInMilliseconds(params.Parameters.IterationTimeScaleInMilliseconds)
		}

		j.DoneRequest(w, r, "OK")
	} else {
		j.DoneRequest(w, r, "perf cache is nil")
	}
}

func (j *JADE) GetControlLoopParameters(w rest.ResponseWriter, r *rest.Request) {
	if j.PerfCache != nil {
		w.WriteJson(j.ControlLoop.Parameters)
	} else {
		j.DoneRequest(w, r, "perf cache is nil")
	}
}

func (j *JADE) ReceiveResourceScalingAction(w rest.ResponseWriter, r *rest.Request) {
	if j.PerfCache != nil {

		j.log.Op.Printf("scaling resource")
		action := &rm.ScalingAction{}
		err := r.DecodeJsonPayload(action)
		if err != nil {
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		
		j.ControlLoop.PhysicallyExecuteAction(action, j.log.Op.Printf)

		j.DoneRequest(w, r, "OK")
	} else {
		j.DoneRequest(w, r, "perf cache is nil")
	}
}

func (j *JADE) ReceiveResourceScalingResult(w rest.ResponseWriter, r *rest.Request) {
	if j.PerfCache != nil {

		j.log.Op.Printf("scaling resource")
		result := &rm.ScalingResult{}
		err := r.DecodeJsonPayload(result)
		if err != nil {
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		j.ControlLoop.ActionHasBeenPhysicallyExecuted(result)
		
		j.DoneRequest(w, r, "OK")
	} else {
		j.DoneRequest(w, r, "perf cache is nil")
	}
}


func (j *JADE) OperateAutoScalingSwitch(w rest.ResponseWriter, r *rest.Request) {
	if j.PerfCache != nil {

		j.log.Op.Printf("operating auto-scaling switch")
		req := map[string]bool{}
		err := r.DecodeJsonPayload(req)
		if err != nil {
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if value, e := req["enable-auto-scaling"]; e {
			j.ControlLoop.SwitchAutoScaling(value)
		}
		
		j.DoneRequest(w, r, "OK")
	} else {
		j.DoneRequest(w, r, "perf cache is nil")
	}
}

