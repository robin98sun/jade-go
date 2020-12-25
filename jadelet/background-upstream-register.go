package jadelet

import (
	"time"
)

func (j *JADE) retryRegister(msg string, seconds int, retryCnt int) {
	// j.log.Println(msg)
	j.RegisterStatus = msg
	time.Sleep(time.Second * time.Duration(seconds))
	j.Register(retryCnt + 1)
}

// Register to upper node
func (j *JADE) Register(retryCnt int) {
	if retryCnt > 99999999999 {
		j.log.Println("Retried maximum times, will no longer register to upper node")
		return
	}

	j.log.Printf("trying to register to upper node for the [%v]th time", retryCnt+1)

	if j.Config.UpperNode == nil || j.Config.UpperNode.IsAddrEmpty() {
		if j.Config.UpperNode != nil {
			j.MakeUpAddressForNode(j.Config.UpperNode)
		}
		if j.Config.UpperNode == nil || j.Config.UpperNode.IsAddrEmpty() {
			j.retryRegister("Upper node is empty, will retry in 10 seconds", 10, retryCnt+1)
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

	j.sdk.HTTPCommunicate(
		"register to master node",
		sn.Protocol, "PUT", "/$jade$/registerNode",
		un.GetSDKNode(), payload, 0, -1,
	)
}
