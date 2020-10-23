package jadelet

import (
	"aces/jade-go/kernel"
	"encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	"log"
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

func (j *JADE) CollectAcceptances(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	content, req, err := j.ValidateUpstreamRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		log.Println("[acceptances collecter] ERROR of validating feedback of acceptances:", err.Error())
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}
	log.Println("[acceptances collecter] Received feedback of acceptances from", req.NodeID)

	reqInst := &struct {
		Payload *TaskEvalResult `json:"payload,omitempty"`
	}{}
	err = json.Unmarshal(content, reqInst)

	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode task evaluation result: "+err.Error())
		log.Println("[acceptances collecter] ERROR of decoding content of acceptances:", err.Error())
		return
	} else {
		feedback := reqInst.Payload
		subnodeID := req.NodeID
		subnode := j.Subnodes[subnodeID]
		result := NewTaskEvalResult()
		feedbackbytes, _ := json.MarshalIndent(feedback, "", "    ")
		log.Printf("[acceptances collecter] content of acceptances: %v, from node: %v", string(feedbackbytes), req.NodeID)
		for _, taskID := range feedback.Accepted {
			if task := j.taskCache.GetTask(taskID); task != nil {
				// BIG BUG
				status := j.taskCache.Set(task, subnode, "", kernel.TaskStatusAccepted, nil)
				result.AppendTask(taskID, kernel.TaskStatus(status))
				log.Printf("node[%v] accepted task [%v]", req.NodeID, taskID)
			}
		}
		for _, taskID := range feedback.Rejected {
			if task := j.taskCache.GetTask(taskID); task != nil {
				// BIG BUG
				status := j.taskCache.Set(task, subnode, "", kernel.TaskStatusRejected, nil)
				result.AppendTask(taskID, kernel.TaskStatus(status))
				log.Printf("node[%v] rejected task [%v]", req.NodeID, taskID)
			}
		}
		if !result.IsEmpty() {
			j.feedbackTaskAcceptances(result)
		}
	}
	j.DoneRequest(w, r, "OK")
}
