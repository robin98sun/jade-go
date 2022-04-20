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
	j.log.Println("updating configuration")
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
		j.log.Printf("removing old capabilities for old nodekey[%v] while updating configuration", originalNodeKey)
		j.subnodeCapabilityCache.DeleteNode(originalNodeKey)
		j.neighborCapabilityCache.DeleteNode(originalNodeKey)
	}

	if newNodeKey != "" {
		j.log.Printf("setting new capabilities for new nodekey[%v] while updating configuration", newNodeKey)
		j.subnodeCapabilityCache.Set(newNodeKey, j.Config.Capabilities)
		j.neighborCapabilityCache.Set(newNodeKey, j.Config.Capabilities)
	}
	w.WriteJson(c)
	// renew itself in upper node
	// go j.RegisterToUpperNode(0)
}

// AddCapability add a capability to self-node
func (j *JADE) AddCapability(w rest.ResponseWriter, r *rest.Request) {
	nc := jadesdk.Capability{}
	err := r.DecodeJsonPayload(nc)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	j.Config.AddOrUpdateCapability(&nc)
	w.WriteJson(nc)
	// renew itself in upper node
	// go j.RegisterToUpperNode(0)
}

// DeleteCapability delete a capability of self-node
func (j *JADE) DeleteCapability(w rest.ResponseWriter, r *rest.Request) {
	nc := jadesdk.Capability{}
	err := r.DecodeJsonPayload(nc)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	deleted := j.Config.DeleteCapability(nc.Name)
	w.WriteJson(deleted)
	// renew itself in upper node
	// go j.RegisterToUpperNode(0)
}

func (j *JADE) ClearTaskCacheAndStat(w rest.ResponseWriter, r *rest.Request) {
	if j.TaskCache != nil {
		// perform GC on all app pods
		for _, pod := range j.PodCache.GetAllPods() {
			j.HTTPCommunicate(
				"GC on pod "+pod.GetKey(), "DELETE", "/$jade$/GC",
				pod.GetNodeRepresentation(j.Config.SelfNode.Protocol),
				nil,
				0, 10,
			)
		}

		// Clear self cache and perform GC
		j.TaskCache.Clear()
		runtime.GC()
	}
	j.DoneRequest(w, r, "OK")
}

func (j *JADE) ClearPodCache(w rest.ResponseWriter, r *rest.Request) {
	if j.PodCache != nil {
		j.PodCache.Clear()
		runtime.GC()
	}
	j.DoneRequest(w, r, "OK")
}
