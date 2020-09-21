package interfaces

import (
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
