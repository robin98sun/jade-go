package interfaces

import (
	"aces/jade-go/conf"
	"aces/jade-go/provisioner"
	"github.com/ant0ine/go-json-rest/rest"
	"net/http"
)

// ProvisionApp interfaces
func (j *JADE) ProvisionApp(w rest.ResponseWriter, r *rest.Request) {
	p := provisioner.Provision{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	podlist := p.ProvisionApplication()

	w.WriteJson(&podlist)
}

// UpdateConfigurations to configure JADE at runtime
func (j *JADE) UpdateConfigurations(w rest.ResponseWriter, r *rest.Request) {
	c := conf.NewConfiguration()
	err := r.DecodeJsonPayload(c)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	j.Config = c
	w.WriteJson(c)
}
