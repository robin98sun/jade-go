package jadelet

import (
	"encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	"uta.edu/aces/jade-go/kernel"
)

// RegisterNode receive and process node registration
func (j *JADE) RegisterSubnode(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	_, payload, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}

	j.registerNode(JadeNodeTypeSubnode, payload)
	
	// finish the request
	j.DoneRequest(w, r, nil)
	// start the pod queue
	if j.IsCoordinator() {
		// j.PodCache.Lock()
		// defer j.PodCache.Unlock()
		if !j.PodCache.IsBackgroundRoutineStarted {
			j.PodCache.IsBackgroundRoutineStarted = true
			go j.routineForPodQueues(1)
		}
	}
}

func (j *JADE) RegisterNeighbor(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	_, payload, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}

	j.registerNode(JadeNodeTypeNeighbor, payload)
	
	// finish the request
	j.DoneRequest(w, r, nil)
}

func (j *JADE) registerNode(nodeType JadeNodeType, payload *RequestPayload) {
	nodekey := payload.Node.Key()

	j.registrationMutex.Lock()
	defer j.registrationMutex.Unlock()

	nodeCache := j.Subnodes
	if nodeType == JadeNodeTypeNeighbor {
		nodeCache = j.Neighbors
	}
	if _, exists := nodeCache[nodekey]; exists {
		j.log.Printf("updating information for existing %v[%v]", nodeType, nodekey)
	} else {
		j.log.Printf("registering information for new %v[%v]", nodeType, nodekey)
	}

	// Save the sub node in its sub node array
	nodeCache[nodekey] = payload.Node

	// En-cache capabilities
	if len(payload.Capabilities) > 0 {
		if nodeType == JadeNodeTypeSubnode {
			j.subnodeCapabilityCache.Set(nodekey, payload.Capabilities)
		} else if nodeType == JadeNodeTypeNeighbor {
			j.neighborCapabilityCache.Set(nodekey, payload.Capabilities)
		}
	}

	// En-cache capacity
	if payload.Capacity != nil {
		if nodeType == JadeNodeTypeSubnode {
			j.subnodeCapacityCache.Set(nodekey, payload.Capacity, payload.Capacity)
		} else if nodeType == JadeNodeTypeNeighbor {
			j.neighborCapacityCache.Set(nodekey, payload.Capacity, payload.Capacity)
		}
	}

}

func (j *JADE) CollectProvisioning(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	content, req, err := j.ValidateUpstreamRequest(w, r)
	// j.Lock()
	// defer j.Unlock()
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
	}
	feedback := reqInst.Payload
	if feedback.Pod == nil {
		// task is rejected by sub-node or provisioning failed
		j.log.Printf("[provisioning collector] sub-node{%v} failed to provision pod for module{%v} of task{%v}", feedback.NodeKey, feedback.ModuleName, feedback.TaskKey)
		// forward the rejection upword
		j.TaskCache.RejectTask(feedback.TaskKey)
		j.feedbackProvisioning(&TaskProvisioningResult{
			NodeKey:    j.Config.SelfNode.Key(),
			TaskKey:    feedback.TaskKey,
			ModuleName: feedback.ModuleName,
			Pod:        nil,
			SubtaskKey: "",
		})
	} else {
		j.log.Printf("[provisioning collector] caching pod[%v] on node[%v] for task[%v], module[%v]", feedback.Pod.GetKey(), feedback.NodeKey, feedback.TaskKey, feedback.ModuleName)
		j.TaskCache.CacheTaskForSubnode(feedback.TaskKey, j.GetNodeInControl(feedback.NodeKey), feedback.ModuleName, nil, feedback.Pod, string(kernel.AppModuleWorker), feedback.SubtaskKey, j.log.Printf)
		taskItem := j.TaskCache.GetTask(feedback.TaskKey, true)
		whetherEnqueue := true
		if feedback.ModuleName == string(kernel.AppModuleAggregator) {
			whetherEnqueue = false
		}
		j.PodCache.SetPodForApplication(
			feedback.NodeKey, taskItem.Task.Application,
			feedback.ModuleName, taskItem.Task.Requirements.Allocations[feedback.ModuleName],
			feedback.Pod, whetherEnqueue,
		)
		// check if the task is ready for dispatching
		j.checkTaskStatus(feedback.TaskKey)
		// if it is ready, then dispatch the task for it
	}
	j.DoneRequest(w, r, "OK")
}
