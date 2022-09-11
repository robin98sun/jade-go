package jadelet

import (
	"encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	ds "uta.edu/aces/jadesdk/data_structure"
)

// interface
func (j *JADE) ContainerProvisioner(w rest.ResponseWriter, r *rest.Request) {
	content, _, err := j.ValidateRequest(w, r)
	if err != nil {
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}
	// re-decode
	reqInst := &struct {
		Payload *ds.ContainerProvisionTask `json:"payload,omitempty"`
	}{}
	err = json.Unmarshal(content, reqInst)
	if err != nil {
		j.PeacefulFatalRequest(w, r, "can not decode task list: "+err.Error())
		return
	} else {
		if reqInst == nil || reqInst.Payload == nil || len(reqInst.Payload.Containers) == 0 {
			j.PeacefulFatalRequest(w, r, "container provisioner received empty payload")
			return
		}

		res := j.processContainerProvisionTask(reqInst.Payload)

		j.DoneRequest(w, r, res)
	}

}

// provision containers
func (j *JADE) processContainerProvisionTask(task *ds.ContainerProvisionTask) int {
	result := 0

	if task == nil || len(task.Containers) == 0 {return result}

	// temporaryAppCache := make(map[string]map[string]*ds.ContainerProvisionItem)




	return result
}
