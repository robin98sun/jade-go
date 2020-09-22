package interfaces

import (
	// "aces/jade-go/conf"
	// "fmt"
	// "aces/jade-go/kube"
	"github.com/ant0ine/go-json-rest/rest"
	"net/http"
)

// ShowConfigurations show current JADE runtime configurations
func (j *JADE) ShowConfigurations(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(j.Config)
}

// ShowPodInfo show current pod information
func (j *JADE) ShowPodInfo(w rest.ResponseWriter, r *rest.Request) {
	status, err := j.Kube.PodInfo(j.Config.SelfNode.Hostname, j.Config.SelfNode.Namespace, j.Config.SelfNode.PodName)
	if err == nil {
		w.WriteJson(status)
	} else {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ShowJadelet show current JADE instance in-memory data structure
func (j *JADE) ShowJadelet(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(j.RegisterStatus)
}
