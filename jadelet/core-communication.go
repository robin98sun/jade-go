package jadelet

import (
	"aces/jade-go/kernel"
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (j *JADE) retryHTTPCommunication(op string, method string, path string, targetNode *kernel.Node, payload interface{}, logMsg string, seconds int, retryCnt int, retrylimitation int) (interface{}, error) {
	if logMsg != "" {
		log.Println(logMsg)
	}
	time.Sleep(time.Second * time.Duration(seconds))
	return j.HTTPCommunicate(op, method, path, targetNode, payload, retryCnt+1, retrylimitation)
}

// HTTPCommunicate access target node, if retry >= 0, then enable retry mode, if retry < 0, then disable retry mode
func (j *JADE) HTTPCommunicate(operationName string, method string, path string, targetNode *kernel.Node, payload interface{}, retryCnt int, retryLimitation int) (interface{}, error) {
	if targetNode == nil || (strings.ToLower(targetNode.Protocol) != "http" && strings.ToLower(targetNode.Protocol) != "https") {
		msg := "ERROR: invalid target node for " + operationName
		log.Println(msg)
		return nil, errors.New(msg)
	}
	if retryCnt > retryLimitation {
		msg := "Retried maximum times: " + strconv.Itoa(retryCnt) + ", will no longer retry " + operationName
		log.Println(msg)
		return nil, errors.New(msg)
	}

	tailstr := ""
	if retryCnt > 0 {
		tailstr = ", retry count: " + strconv.Itoa(retryCnt)
	}
	log.Println("[comm] "+operationName+" started toward target node:", targetNode.Key(), tailstr)
	if targetNode.IsAddrEmpty() {
		j.MakeUpAddressForNode(targetNode)
		if targetNode.IsAddrEmpty() {
			msg := "target node is empty, will retry in 30 seconds"
			return j.retryHTTPCommunication(operationName, method, path, targetNode, payload, msg, 30, retryCnt+1, retryLimitation)
		}
	}

	reqbody, err := json.Marshal(payload)
	if err != nil {
		msg := "ERROR during encoding payload: " + err.Error() + ", will retry in 30 seconds"
		return j.retryHTTPCommunication(operationName, method, path, targetNode, payload, msg, 30, retryCnt+1, retryLimitation)
	}
	// Send the register information to upper node
	req, err := http.NewRequest(strings.ToUpper(method), targetNode.URL()+path, bytes.NewBuffer(reqbody))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)

	if err != nil {
		msg := "Error when sending http request, will retry after 30 seconds: " + err.Error()
		return j.retryHTTPCommunication(operationName, method, path, targetNode, payload, msg, 30, retryCnt+1, retryLimitation)
	}

	if res == nil || res.Body == nil {
		msg := "Error of the communication for the response is nil, will retry after 30 seconds"
		return j.retryHTTPCommunication(operationName, method, path, targetNode, payload, msg, 30, retryCnt+1, retryLimitation)
	}

	// parse the response message of upper node for registering
	resMsg := ResponsePayload{}
	json.NewDecoder(res.Body).Decode(&resMsg)
	if resMsg.Error != "" {
		msg := "target node responded ERROR message: " + resMsg.Error + ", will retry " + operationName + " registering in 30 seconds"
		return j.retryHTTPCommunication(operationName, method, path, targetNode, payload, msg, 30, retryCnt+1, retryLimitation)
	} else {
		if resMsg.Status == "OK" {
			log.Println("[comm] "+operationName+" complete with target node:", targetNode.Key())
			return resMsg.Payload, nil
		} else {
			msg := "target node responded abnormal status: " + resMsg.Status + ", will retry " + operationName + " in 30 seconds"
			return j.retryHTTPCommunication(operationName, method, path, targetNode, payload, msg, 30, retryCnt+1, retryLimitation)
		}
	}
}
