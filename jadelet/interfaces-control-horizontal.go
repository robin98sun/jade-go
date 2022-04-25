package jadelet

import (
	// "encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	"uta.edu/aces/jade-go/kernel"
	"encoding/json"
)

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

func (j *JADE) ListNeighbors(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	content, _, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}

	reqInst := &struct {
		Payload *kernel.Requirements
	}{}
	err = json.Unmarshal(content, reqInst)

	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode requirements of listing eligible neighbors: "+err.Error())
		j.log.Println("[registry] ERROR of decoding content of requirements:", err.Error())
		return
	}

	requirements := reqInst.Payload

	nodekeys := j.selectAvaiableNodes(JadeNodeTypeNeighbor, requirements)

	var nodes []*kernel.Node
	if len(nodekeys) > 0 {
		j.registryMutex.Lock()
		defer j.registryMutex.Unlock()

		for _, nodeKey := range nodekeys {
			if _ , e := j.Neighbors[nodeKey]; e {
				nodes = append(nodes, j.Neighbors[nodeKey])
			} else if nodeKey == j.Config.SelfNode.Key() {
				nodes = append(nodes, j.Config.SelfNode)
			}
			j.log.Printf("got eligible neighbor [%v]: %v", nodeKey, nodes[len(nodes)-1])
		}
	}
	
	// finish the request
	j.DoneRequest(w, r, nodes)
}

func (j *JADE) NeighborInquiry(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	content, _, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}

	reqInst := &struct {
		Payload *kernel.Requirements
	}{}
	err = json.Unmarshal(content, reqInst)

	if err != nil {
		j.PeacefulFatalRequest(w, r, "Can not decode requirements of listing eligible neighbors: "+err.Error())
		j.log.Println("[registry] ERROR of decoding content of requirements:", err.Error())
		return
	}
	
	// finish the request
	j.DoneRequest(w, r, nil)
}

func (j *JADE) NeighborGossip(w rest.ResponseWriter, r *rest.Request) {
	// Validation
	_, _, err := j.ValidateRequest(w, r)
	if err != nil {
		// the request has been rejected by validator
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}
	
	// finish the request
	j.DoneRequest(w, r, nil)
}
