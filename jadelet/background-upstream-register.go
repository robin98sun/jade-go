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

	retryInterval := 300

	if (nodeType == JadeNodeTypeRegistryNode && !j.Config.RegistryNode.IsAddrEmpty()) || (nodeType == JadeNodeTypeUpperNode && !j.Config.UpperNode.IsAddrEmpty()) {
		j.log.Printf("registering to %v node", nodeType)

		// j.log.Printf("trying to register to upper node for the [%v]th time", retryCnt+1)
		// Find UpperNode IP in cluster
		tn := j.Config.UpperNode
		if nodeType == JadeNodeTypeRegistryNode {
			tn = j.Config.RegistryNode
		}
		if tn == nil || tn.IsAddrEmpty() {
			j.MakeUpAddressForNode(tn)
		}
		if tn == nil || tn.IsAddrEmpty() {
			j.retryRegister(nodeType, fmt.Sprintf("%v node is empty, will retry in 60 seconds", nodeType), 60, retryCnt+1)
			return
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
		capacity := j.Config.Capacity
		if nodeType == JadeNodeTypeRegistryNode {
			capacity = nil
		}
		payload := j.GeneratePayloadOfRequest(tn, nil, j.subnodeCapabilityCache.GetAllCapabilities(), capacity)
		payload.Node = sn.MiniNode()
		payload.NodeID = sn.Key()

		apiPath := "/$jade$/registerSubnode"
		if nodeType == JadeNodeTypeRegistryNode {
			apiPath = "/$jade$/registerNeighbor"
		}
		j.sdk.HTTPCommunicate(
			fmt.Sprintf("register to %v node", nodeType),
			sn.Protocol, "PUT", apiPath,
			tn.GetSDKNode(), payload, 0, -1,
		)

	} else {
		j.log.Println("Can NOT register to %v node because it is empty in the configuration for now", nodeType)
	}

	
	j.log.Printf("going to redo the registration to %v in %v seconds", nodeType, retryInterval)
	j.retryRegister(nodeType, fmt.Sprintf("heartbeat to %v node", nodeType), retryInterval, int64(0))
}
