package jadelet

import (
	// "aces/jade-go/conf"
	"aces/jade-go/kernel"
	"aces/jade-go/provisioner"
	"github.com/ant0ine/go-json-rest/rest"
	"log"
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

// TaskReceiver task receiver
func (j *JADE) TaskReceiver(w rest.ResponseWriter, r *rest.Request) {
	taskList := []kernel.Task{}
	err := r.DecodeJsonPayload(&taskList)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	validTasks := []*kernel.Task{}
	for _, task := range taskList {
		if task.Valid() {
			if task.Key == "" {
				task.GenKey()
			}
			validTasks = append(validTasks, &task)
		} else {
			log.Println("WARN: received an invalid task")
		}
	}
	if len(validTasks) > 0 {
		go j.evaluateTasks(validTasks)
	}
	payload := struct {
		ReceivedTasks int `json:"receivedTasks:omitempty"`
	}{
		ReceivedTasks: len(validTasks),
	}
	w.WriteJson(&ResponsePayload{Status: "OK", Payload: &payload})
}
