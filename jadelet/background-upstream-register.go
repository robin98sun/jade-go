package jadelet

import (
	"time"
	"fmt"
)

func (j *JADE) retryRegister(nodeType JadeNodeType, msg string, seconds int, retryCnt int64) {
	// j.log.Println(msg)
	j.RegisterStatus = msg
	time.Sleep(time.Second * time.Duration(seconds))
	j.RegisterToNode(nodeType, retryCnt)
}

// Register to upper node
func (j *JADE) RegisterToNode(nodeType JadeNodeType, retryPointer int64) {
	// if retryCnt > 999999999 {
	// 	j.log.Println("Retried maximum times, will no longer register to upper node")
	// 	return
	// }
	retryCnt := retryPointer
	if retryCnt > int64(999999999999) {
		retryCnt = int64(1)
	}

	// j.log.Printf("trying to register to upper node for the [%v]th time", retryCnt+1)
	if nodeType == JadeNodeTypeUpperNode {

	}
	if nodeType == JadeNodeTypeUpperNode && (j.Config.UpperNode == nil || j.Config.UpperNode.IsAddrEmpty()) {
		if j.Config.UpperNode != nil {
			j.MakeUpAddressForNode(j.Config.UpperNode)
		}
		if j.Config.UpperNode == nil || j.Config.UpperNode.IsAddrEmpty() {
			j.retryRegister(nodeType, fmt.Sprintf("%v node is empty, will retry in 60 seconds", nodeType), 60, retryCnt+1)
			return
		}
	} else if nodeType == JadeNodeTypeRegistryNode && (j.Config.RegistryNode == nil || j.Config.RegistryNode.IsAddrEmpty()) {
		if j.Config.RegistryNode != nil {
			j.MakeUpAddressForNode(j.Config.RegistryNode)
		}
		if j.Config.RegistryNode == nil || j.Config.RegistryNode.IsAddrEmpty() {
			j.retryRegister(nodeType, fmt.Sprintf("%v node is empty, will retry in 60 seconds", nodeType), 60, retryCnt+1)
			return
		}
	}

	// Find UpperNode IP in cluster
	tn := j.Config.UpperNode
	if nodeType == JadeNodeTypeRegistryNode {
		tn = j.Config.RegistryNode
	}

	// Check self-node accessibility
	sn := j.Config.SelfNode
	if sn.IsAddrEmpty() {
		j.MakeUpAddressForNode(sn)
		if sn.IsAddrEmpty() {
			msg := fmt.Sprintf("ERROR when preparing self-node address for registering on %v node, the self-node address is empty, will retry in 10 seconds", nodeType)
			j.retryRegister(nodeType, msg, 10, retryCnt+1)
			return
		}
	}

	// Prepare payload of registering
	payload := j.GeneratePayloadOfRequest(nil, nil, j.Config.Capabilities, j.Config.Capacity)
	payload.Node = sn.MiniNode()
	payload.NodeID = sn.Key()

	apiPath := "/$jade$/registerSubnode"
	if nodeType == JadeNodeTypeRegistryNode {
		apiPath = "/$jade$/registerNeighbor"
	}
	j.sdk.HTTPCommunicate(
		"register to master node",
		sn.Protocol, "PUT", apiPath,
		tn.GetSDKNode(), payload, 0, -1,
	)

	j.retryRegister(nodeType, fmt.Sprintf("heartbeat to %v node", nodeType), 60, int64(0))
}
