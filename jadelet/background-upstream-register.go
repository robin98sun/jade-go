package jadelet

import (
	// "aces/jade-go/conf"
	// "aces/jade-go/kernel"
	// "aces/jade-go/kube"
	"bytes"
	"encoding/json"
	// "errors"
	// "github.com/ant0ine/go-json-rest/rest"
	"log"
	"net/http"
	"time"
)

func (j *JADE) retryRegister(msg string, seconds int, retryCnt int) {
	log.Println(msg)
	j.RegisterStatus = msg
	time.Sleep(time.Second * time.Duration(seconds))
	j.Register(retryCnt + 1)
}

// Register to upper node
func (j *JADE) Register(retryCnt int) {
	// if retryCnt > 100 {
	// 	log.Println("Retried maximum times, will no longer register to upper node")
	// 	return
	// }

	if j.Config.UpperNode == nil || j.Config.UpperNode.IsAddrEmpty() {
		if j.Config.UpperNode != nil {
			j.MakeUpAddressForNode(j.Config.UpperNode)
		}
		if j.Config.UpperNode == nil || j.Config.UpperNode.IsAddrEmpty() {
			j.retryRegister("Upper node is empty, will retry in 30 seconds", 30, retryCnt+1)
			return
		}
		// }
	}
	// Find UpperNode IP in cluster
	un := j.Config.UpperNode

	// Check self-node accessibility
	sn := j.Config.SelfNode
	if sn.IsAddrEmpty() {
		j.MakeUpAddressForNode(sn)
		if sn.IsAddrEmpty() {
			msg := "ERROR when preparing self-node address for registering on upper node, the self-node address is empty, will retry in 10 seconds"
			j.retryRegister(msg, 10, retryCnt+1)
			return
		}
	}

	// Prepare payload of registering
	payload := j.GeneratePayloadOfRequest(nil, nil, j.Config.Capabilities, j.Config.Capacity)
	payload.Node = sn.MiniNode()
	payload.NodeID = sn.Key()
	reqbody, err := json.Marshal(payload)
	if err != nil {
		msg := "ERROR during encoding self-node: " + err.Error() + ", will retry in 10 seconds"
		j.retryRegister(msg, 10, retryCnt+1)
		return
	}
	// Send the register information to upper node
	req, err := http.NewRequest("PUT", un.URL()+"/$jade$/registerNode", bytes.NewBuffer(reqbody))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)

	if err != nil {
		msg := "Error when registering, will retry after 10 seconds: " + err.Error()
		j.retryRegister(msg, 10, retryCnt+1)
		return
	}

	if res == nil || res.Body == nil {
		msg := "Error when registering, the response is nil, will retry after 10 seconds"
		j.retryRegister(msg, 10, retryCnt+1)
		return
	}

	// parse the response message of upper node for registering
	var resMsg map[string]string
	json.NewDecoder(res.Body).Decode(&resMsg)
	if val, ok := resMsg["Error"]; ok {
		msg := "Upper node responded ERROR message: " + val + ", will retry registering in 10 seconds"
		j.retryRegister(msg, 10, retryCnt+1)
		return
	} else {
		if val, ok := resMsg["status"]; ok && val == "OK" {
			j.RegisterStatus = val
			log.Println("Registered in upper node: ", resMsg)
		} else if ok {
			msg := "Upper node responded abnormal message: " + val + ", will retry registering in 10 seconds"
			j.retryRegister(msg, 10, retryCnt+1)
			return
		} else {
			msg := "Upper node responded message did not contain register status, will retry registering in 10 seconds"
			j.retryRegister(msg, 10, retryCnt+1)
			return
		}
	}
}
