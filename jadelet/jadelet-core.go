package jadelet

import (
	"encoding/json"
	"io/ioutil"
	"errors"
	"github.com/ant0ine/go-json-rest/rest"
	"net/http"
	"sync"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/kube"
	"uta.edu/aces/jade-go/provisioner"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jade-go/scheduler/resource"
	"uta.edu/aces/jade-go/scheduler/perfstat"
	"uta.edu/aces/jade-go/scheduler/chef"
	"uta.edu/aces/jadesdk"
	"fmt"
)

type JadeNodeType string
const (
	JadeNodeTypeUpperNode 	JadeNodeType = "upper"
	JadeNodeTypeRegistryNode		     = "registry"
	JadeNodeTypeSelfNode 			  	 = "self"
	JadeNodeTypeSubnode 			  	 = "subnode"
	JadeNodeTypeNeighbor 			  	 = "neighbor"
)

// JADE to instantiate JADE memory structure
type JADE struct {
	Config          *kernel.Conf             `json:"config"`
	Provisioner     *provisioner.Provisioner `json:"provisioner"`
	Kube            *kube.KubeClient         `json:"kube"`
	Subnodes        map[string]*kernel.Node  `json:"subnodes"`
	Neighbors       map[string]*kernel.Node  `json:"neighbors"`
	RegisterStatus  string                   `json:"registerStatus"`
	CapacityStatus  *kernel.CapacityStatus   `json:"capacityStatus"`
	subnodeCapabilityCache *kernel.CapabilityCache
	subnodeCapacityCache   *kernel.CapacityCache
	neighborCapabilityCache *kernel.CapabilityCache
	neighborCapacityCache   *kernel.CapacityCache
	eligibleNeighborCache   *kernel.EligibleNeighborCache
	log             *kernel.Logger
	TaskCache       *scheduler.TaskCache `json:"taskCache"`
	// PodCache        *scheduler.PodCache  `json:"podCache"`
	ResourceCache   *resource.ResourceCache `json:"resourceCache"`
	PerfCache 		*perfstat.PerfCache `json:"perfCache"`
	Chef            *chef.TakoyakiChef `json:"chef"`
	mutex           *sync.Mutex
	sdk             *jadesdk.JadeSDK
	dist            *scheduler.Dist
	registryMutex 	*sync.Mutex
}

func NewJadelet() *JADE {
	j := &JADE{}
	j.Init()
	return j
}

func (j *JADE) Lock() {
	j.mutex.Lock()
}

func (j *JADE) Unlock() {
	j.mutex.Unlock()
}

func (j *JADE) Verbose(on bool) {
	j.log.Debug.Enabled = on
	j.sdk.Verbose(on)
}

func (j *JADE) PrintConfig() {
	j.log.Op.Println("configurations from environment:")
	j.log.Op.Println("registry node:")
	j.log.Op.Println(j.Config.RegistryNode)
	j.log.Op.Println("")
	j.log.Op.Println("upper node:")
	j.log.Op.Println(j.Config.UpperNode)
	j.log.Op.Println("")
	j.log.Op.Println("self node:")
	j.log.Op.Println(j.Config.SelfNode)
	j.log.Op.Println("")
	j.log.Op.Println("capacity:")
	j.log.Op.Println(j.Config.Capacity)
	j.log.Op.Println("")
	j.log.Op.Println("capabilities:")
}

func (j *JADE) PrintCapabilities() {
	for _, c := range j.Config.Capabilities {
		j.log.Op.Println(c)
	}
}

func (j *JADE) HasRegistry() bool {
	return j.Config != nil && j.Config.RegistryNode != nil && !j.Config.RegistryNode.IsAddrEmpty()
}

func (j *JADE) GetNodeInControl(nodeID string) *kernel.Node {

	if nodeID == "" || j == nil || len(j.Subnodes) == 0 {
		return nil
	}
	if nodeID == j.Config.SelfNode.Key() {
		return j.Config.SelfNode
	} else if n, exists := j.Subnodes[nodeID]; exists {
		return n
	}
	return nil
}

func (j *JADE) IsSelfNode(nodeID string) bool {
	if nodeID == j.Config.SelfNode.Key() {
		return true
	}
	return false
}

func (j *JADE) SelfNodeKey() string {
	if j == nil || j.Config == nil || j.Config.SelfNode == nil {
		return ""
	}
	return j.Config.SelfNode.Key()
}

func (j *JADE) HasUpperNode() bool {
	return j.Config != nil && j.Config.UpperNode != nil && !j.Config.UpperNode.IsAddrEmpty()
}

func (j *JADE) IsCoordinator() bool {
	if len(j.Subnodes) > 0 {
		return true
	}
	return false
}

func (j *JADE) IsTopmostMaster() bool {
	if j.Config.UpperNode.IsAddrEmpty() {
		return true
	}
	return false
}

func (j *JADE) IsLeaf() bool {
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
type RequestPayload struct {
	Payload      interface{}           `json:"payload,omitempty"`
	Token        string                `json:"token,omitempty"`
	Node         *kernel.Node          `json:"node,omitempty"`
	NodeID       string                `json:"nodeId,omitempty"`
	Capabilities []*jadesdk.Capability `json:"capabilities,omitempty"`
	Capacity     *kernel.Capacity      `json:"capability,omitempty"`
}

// ResponsePayload for all requests
type ResponsePayload struct {
	Status  string      `json:"status,omitempty"`
	Error   string      `json:"error,omitempty"`
	Payload interface{} `json:"payload,omitempty"`
}

func DecodeRequestWithoutClosing(r *rest.Request, v interface{}) ([]byte, error) {
	content, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		return nil, err
	}
	if len(content) == 0 {
		return nil, errors.New("JSON payload is empty")
	}
	err = json.Unmarshal(content, v)
	if err != nil {
		return nil, err
	}
	return content, nil
}

// ValidateRequest receive and process node registration
func (j *JADE) ValidateRequest(w rest.ResponseWriter, r *rest.Request) ([]byte, *RequestPayload, error) {
	req := &RequestPayload{}
	content, err := DecodeRequestWithoutClosing(r, req)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return content, req, err
	}
	// Check token
	if req.Token != j.Config.SelfNode.Token {
		err := errors.New("Unauthorized request")
		rest.Error(w, err.Error(), http.StatusForbidden)
		return nil, req, err
	}
	return content, req, nil
}

// ValidateUpstreamRequest receive and process node registration
func (j *JADE) ValidateUpstreamRequest(w rest.ResponseWriter, r *rest.Request) ([]byte, *RequestPayload, error) {
	content, payload, err := j.ValidateRequest(w, r)
	// Check node information
	subnodeExists := true
	if payload.NodeID == "" {
		subnodeExists = false
	} else if _, exists := j.Subnodes[payload.NodeID]; !exists {
		subnodeExists = false
		msgstr := fmt.Sprintf("WARNING: unknown visitor [%v] claiming as a subnode", payload.NodeID)
		// for nid, _ := range j.Subnodes {
		// 	msgstr = fmt.Sprintf("%v %v,", msgstr, nid)
		// }
		j.log.Op.Printf(msgstr)
	}

	if !subnodeExists {
		err = errors.New(fmt.Sprintf("Unknown visitor: %v", payload.NodeID))
		rest.Error(w, err.Error(), http.StatusForbidden)
		return nil, payload, err
	}
	return content, payload, err
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

// GeneratePayloadOfRequest generate payload of request
func (j *JADE) GeneratePayloadOfRequest(targetNode *kernel.Node, thePayload interface{}, capabilities []*jadesdk.Capability, capacity *kernel.Capacity) *RequestPayload {
	payload := RequestPayload{
		Token:  j.Config.UpperNode.Token,
		NodeID: j.Config.SelfNode.Key(),
	}
	if thePayload != nil {
		payload.Payload = thePayload
	}
	if targetNode != nil {
		payload.Token = targetNode.Token
	}
	if capabilities != nil && len(capabilities) > 0 {
		payload.Capabilities = capabilities
		// for _, c := range capabilities {
			// payload.Capabilities = append(payload.Capabilities, c.MiniCapability())
		// }
	}
	if capacity != nil {
		payload.Capacity = capacity
	}
	return &payload
}
