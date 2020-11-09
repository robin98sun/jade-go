package jadelet

import (
	"aces/jade-go/scheduler"
	"encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	// "strings"
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

func (j *JADE) CollectProvisioning(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	content, req, err := j.ValidateUpstreamRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.log.Println("[provisioning collecter] ERROR of validating feedback of provisioning:", err.Error())
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}
	j.log.Println("[provisioning collecter] Received feedback of provisioning from", req.NodeID)

	reqInst := &struct {
		Payload *TaskProvisioningResult `json:"payload,omitempty"`
	}{}
	err = json.Unmarshal(content, reqInst)

	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode task provisioning result: "+err.Error())
		j.log.Println("[provisioning collecter] ERROR of decoding content of provisioning:", err.Error())
		return
	} else {
		feedback := reqInst.Payload
		j.TaskCache.CacheTaskForSubnode(feedback.TaskKey, j.GetNodeInControl(feedback.NodeKey), feedback.ModuleName, nil, feedback.Pod)
		j.TaskCache.CheckTask(feedback.TaskKey)
	}
	j.DoneRequest(w, r, "OK")
}

func (j *JADE) CollectAppMsg(w rest.ResponseWriter, r *rest.Request) {
	var msg struct {
		TaskID    string                 `json:"taskId,omitempty"`
		SubtaskID string                 `json:"subtaskId,omitempty"`
		Status    scheduler.TaskStatus   `json:"status,omitempty"`
		Updates   map[string]interface{} `json:"updates,omitempty"`
	}
	err := r.DecodeJsonPayload(msg)
	if err == nil {
		bs, _ := json.MarshalIndent(msg, "", "    ")
		j.log.Println("Received application message:", string(bs))
		if msg.TaskID != "" && msg.SubtaskID != "" {
			j.TaskCache.SaveResultFromApp(msg.TaskID, msg.SubtaskID, msg.Status, msg.Updates)
			w.WriteJson(map[string]string{
				"status":  "OK",
				"payload": "message received",
			})
			return
		}
	}
	w.WriteJson(map[string]string{
		"error": "invalid message",
	})
}
