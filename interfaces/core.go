package interfaces

import (
	"aces/jade-go/conf"
	"aces/jade-go/kernel"
	"aces/jade-go/kube"
	"bytes"
	"encoding/json"
	"errors"
	"github.com/ant0ine/go-json-rest/rest"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// JADE to instantiate JADE memory structure
type JADE struct {
	Config         *conf.Conf              `json:"config"`
	Kube           *kube.KubeClient        `json:"kube"`
	SubNodes       map[string]*kernel.Node `json:"subnodes"`
	RegisterStatus string                  `json:"registerStatus"`
}

// RequestPayload for all requests
type RequestPayload struct {
	Token string       `json:"token"`
	Node  *kernel.Node `json:"node"`
}

// ResponsePayload for all requests
type ResponsePayload struct {
	Status string `json:"status"`
}

// ValidateRequest receive and process node registration
func (j *JADE) ValidateRequest(w rest.ResponseWriter, r *rest.Request) (*RequestPayload, error) {
	payload := RequestPayload{}
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
func (j *JADE) DoneRequest(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(ResponsePayload{Status: "OK"})
}

// Init to do initializing work
func (j *JADE) Init() {
	// Initialize caches and queues
	j.SubNodes = make(map[string]*kernel.Node)
	// read environment variables into config
	j.Config = conf.ReadConfFromEnv()
	log.Println("configurations from environment:")
	log.Println(j.Config)
	log.Println("")
	// setup k8s client instance
	clients := kube.KubeClient{}
	clients.Init()
	j.Kube = &clients
	// Register to upper node
	go j.Register(0)
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

// Register to upper node
func (j *JADE) Register(retryCnt int) {
	if retryCnt > 10 {
		log.Println("Retried maximum times, will no longer register to upper node")
		return
	}

	if j.Config.UpperNode == nil || j.Config.UpperNode.IsAddrEmpty() {
		if j.Config.UpperNode != nil {
			j.MakeUpAddressForNode(j.Config.UpperNode)
		}
		if j.Config.UpperNode == nil || j.Config.UpperNode.IsAddrEmpty() {
			log.Println("Upper node is empty, will retry in 30 seconds")
			time.Sleep(time.Second * 30)
			j.Register(retryCnt + 1)
			return
		}
		// }
	}
	// Find UpperNode IP in cluster
	un := j.Config.UpperNode

	// Prepare payload of registering
	payload := RequestPayload{
		Token: un.Token,
		Node:  j.Config.SelfNode,
	}
	reqbody, err := json.Marshal(payload)
	if err != nil {
		msg := "ERROR during encoding self-node: " + err.Error()
		j.RegisterStatus = msg
		log.Fatalln(msg)
		return
	}
	// Send the register information to upper node
	req, err := http.NewRequest("PUT", un.URL()+"/$jade$/registerNode", bytes.NewBuffer(reqbody))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)

	if err != nil {
		msg := "Error when registering, will retry after 10 seconds: " + err.Error()
		log.Println(msg)
		j.RegisterStatus = msg
		time.Sleep(time.Second * 10)
		j.Register(retryCnt + 1)
		return
	}

	if res == nil || res.Body == nil {
		msg := "Error when registering, the response is nil"
		log.Println(msg)
		return
	}

	// parse the response message of upper node for registering
	var resMsg map[string]string
	json.NewDecoder(res.Body).Decode(&resMsg)
	if val, ok := resMsg["Error"]; ok {
		msg := "Upper node responded ERROR message: " + val
		log.Println(msg)
		j.RegisterStatus = msg
		log.Println("will retry registering in 10 seconds")
		time.Sleep(time.Second * 10)
		j.Register(retryCnt + 1)
		return
	} else {
		log.Println("Registered in upper node: ", resMsg)
		regMsg, _ := ioutil.ReadAll(res.Body)
		j.RegisterStatus = string(regMsg)
	}
	// res.Body.Close()
}
