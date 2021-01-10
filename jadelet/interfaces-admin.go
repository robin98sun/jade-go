package jadelet

import (
	"github.com/ant0ine/go-json-rest/rest"
	"net/http"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jadesdk"
)

// UpdateConfigurations to configure JADE at runtime
func (j *JADE) UpdateConfigurations(w rest.ResponseWriter, r *rest.Request) {
	c := kernel.NewConfiguration()
	err := r.DecodeJsonPayload(c)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	j.Config = c
	w.WriteJson(c)
	// renew itself in upper node
	go j.Register(0)
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
	go j.Register(0)
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
	go j.Register(0)
}

func (j *JADE) ClearTaskCacheAndStat(w rest.ResponseWriter, r *rest.Request) {
	if j.TaskCache != nil {
		j.TaskCache.Clear()
	}
	j.DoneRequest(w, r, "OK")
}

func (j *JADE) ClearPodCache(w rest.ResponseWriter, r *rest.Request) {
	if j.PodCache != nil {
		j.PodCache.Clear()
	}
	j.DoneRequest(w, r, "OK")
}
