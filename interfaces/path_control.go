package interfaces

import (
	// "aces/jade-go/kernel"
	"github.com/ant0ine/go-json-rest/rest"
	// "net/http"
)

// RegisterNode receive and process node registration
func (j *JADE) RegisterNode(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	payload, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		return
	}
	// Save the sub node in its sub node array
	subnode := payload.Node
	j.SubNodes[subnode.Key()] = subnode
	// finish the request
	j.DoneRequest(w, r)
}

// Heartbeat between nodes
func (j *JADE) Heartbeat(w rest.ResponseWriter, r *rest.Request) {

}
