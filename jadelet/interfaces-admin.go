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
}
