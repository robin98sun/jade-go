package jadelet

import (
	"aces/jade-go/kernel"
	"aces/jade-go/kube"
	// "bytes"
	// "encoding/json"
	"errors"
	"github.com/ant0ine/go-json-rest/rest"
	// "log"
	"net/http"
	// "time"
)

// JADE to instantiate JADE memory structure
type JADE struct {
	Config          *kernel.Conf            `json:"config"`
	Kube            *kube.KubeClient        `json:"kube"`
	Subnodes        map[string]*kernel.Node `json:"subnodes"`
	RegisterStatus  string                  `json:"registerStatus"`
	capabilityCache *kernel.CapabilityCache
	capacityCache   *kernel.CapacityCache
	taskCache       *kernel.TaskCache
}

func (j *JADE) HasUpperNode() bool {
	return j.Config != nil && j.Config.UpperNode != nil && !j.Config.UpperNode.IsAddrEmpty()
}

func (j *JADE) IsAggregator() bool {
	if len(j.Subnodes) > 0 {
		return true
	}
	return false
}

func (j *JADE) IsLinker() bool {
	if len(j.Subnodes) == 1 {
		return true
	}
	return false
}

func (j *JADE) IsMaster() bool {
	if j.Config.UpperNode.IsAddrEmpty() {
		return true
	}
	return false
}

func (j *JADE) IsWorker() bool {
	if len(j.Subnodes) == 0 {
		return true
	}
	return false
}

// MakeUpAddressForNode to make up empty address for a node
func (j *JADE) MakeUpAddressForNode(n *kernel.Node) {
	if n.Address != "" && n.Port != 0 {
		return
	}
	if n.Port == 0 && n.Namespace != "" && n.ServiceExternal != "" {
		n.Port = j.Kube.FindExternalPort(n.Namespace, n.ServiceExternal)
	}
	if n.Address == "" && n.Hostname != "" {
		n.Address = j.Kube.FindExternalIP(n.Hostname)
	}

}

// IsRegistered tell whether the node is registered in upper node
func (j *JADE) IsRegistered() bool {
	return j.RegisterStatus == "OK"
}

// UpstreamRequestPayload for all requests
type UpstreamRequestPayload struct {
	Payload      interface{}          `json:"payload,omitempty"`
	Token        string               `json:"token,omitempty"`
	Node         *kernel.Node         `json:"node,omitempty"`
	NodeID       string               `json:"nodeId,omitempty"`
	Capabilities []*kernel.Capability `json:"capabilities,omitempty"`
	Capacity     *kernel.Capacity     `json:"capability,omitempty"`
}

// ResponsePayload for all requests
type ResponsePayload struct {
	Status  string      `json:"status,omitempty"`
	Error   string      `json:"error,omitempty"`
	Payload interface{} `json:"payload,omitempty"`
}

// ValidateUpstreamRequest receive and process node registration
func (j *JADE) ValidateUpstreamRequest(w rest.ResponseWriter, r *rest.Request) (*UpstreamRequestPayload, error) {
	payload := UpstreamRequestPayload{}
	err := r.DecodeJsonPayload(&payload)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return nil, err
	}
	// Check token
	if payload.Token != j.Config.SelfNode.Token {
		err = errors.New("Invalid request")
		rest.Error(w, err.Error(), http.StatusForbidden)
		return nil, err
	}
	// Check node information
	if payload.Node == nil {
		err = errors.New("Unknown visitor")
		rest.Error(w, err.Error(), http.StatusForbidden)
		return nil, err
	}
	return &payload, err
}

// DoneRequest send a message to the visitor to say everything is done
func (j *JADE) DoneRequest(w rest.ResponseWriter, r *rest.Request, payload interface{}) {
	if payload != nil {
		w.WriteJson(ResponsePayload{
			Status:  "OK",
			Payload: payload,
		})
	} else {
		w.WriteJson(ResponsePayload{Status: "OK"})
	}
}

// PeacefulFatalRequest send an Error message to the visitor to say some business is wrong, without breaking the connection
func (j *JADE) PeacefulFatalRequest(w rest.ResponseWriter, r *rest.Request, msg string) {
	w.WriteJson(ResponsePayload{
		Status: "ERROR",
		Error:  msg,
	})
}

// GenerateUpstreamPayloadOfControlPath generate upstream payload of control path
func (j *JADE) GenerateUpstreamPayloadOfControlPath(thePayload interface{}, node *kernel.Node, capabilities []*kernel.Capability, capacity *kernel.Capacity) *UpstreamRequestPayload {
	payload := UpstreamRequestPayload{
		Token:  j.Config.UpperNode.Token,
		NodeID: j.Config.SelfNode.Key(),
	}
	if thePayload != nil {
		payload.Payload = thePayload
	}
	if node != nil {
		payload.Node = node.MiniNode()
		payload.NodeID = node.Key()
	}
	if capabilities != nil && len(capabilities) > 0 {
		for _, c := range capabilities {
			payload.Capabilities = append(payload.Capabilities, c.MiniCapability())
		}
	}
	if capacity != nil {
		payload.Capacity = capacity
	}
	return &payload
}
