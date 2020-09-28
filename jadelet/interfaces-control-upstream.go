package jadelet

import (
	// "aces/jade-go/kernel"
	"encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	// "net/http"
)

// RegisterNode receive and process node registration
func (j *JADE) RegisterNode(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	_, payload, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}
	// Save the sub node in its sub node array
	nodekey := payload.Node.Key()
	j.Subnodes[nodekey] = payload.Node
	// En-cache capabilities
	j.capabilityCache.Set(nodekey, payload.Capabilities)
	// En-cache capacity
	j.capacityCache.Set(nodekey, payload.Capacity, payload.Capacity)
	// finish the request
	j.DoneRequest(w, r, nil)
}

func (j *JADE) FeedbackAcceptances(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	content, req, err := j.ValidateUpstreamRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}

	reqInst := &struct {
		Payload *TaskEvalResult `json:"payload,omitempty"`
	}{}
	err = json.Unmarshal(content, reqInst)

	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode task evaluation result: "+err.Error())
	} else {
		feedback := reqInst.Payload
		subnodeID := req.NodeID
		subnode := j.Subnodes[subnodeID]
		result := &TaskEvalResult{}
		for _, taskID := range feedback.Accepted {
			status := j.taskCache.Set(taskID, nil, subnode, true, false, false, nil)
			result.AppendTask(taskID, status)
		}
		for _, taskID := range feedback.Rejected {
			status := j.taskCache.Set(taskID, nil, subnode, false, true, false, nil)
			result.AppendTask(taskID, status)
		}
		if !result.IsEmpty() {
			j.feedbackTaskStatus(result)
		}
	}
	j.DoneRequest(w, r, nil)
}
