package jadelet

import (
	"encoding/json"
	"errors"

	rm "uta.edu/aces/jade-go/resource_manager"
	ds "uta.edu/aces/jadesdk/data_structure"
)

func (j *JADE) HTTPCommunicate(
	operationName string, method string, path string, targetNode *ds.Node,
	payload interface{}, retryCnt int, retryLimitation int,
) (interface{}, int, []byte, error) {
	return j.sdk.HTTPCommunicate(
		operationName, targetNode.Protocol, method, path,
		// targetNode.GetSDKNode(),
		targetNode,
		payload, retryCnt, retryLimitation,
	)
}

func (j *JADE) CommReportResourceScalingResult(targetNode *ds.Node, result *rm.ScalingResult) bool {
	_, _, _, err := j.HTTPCommunicate(
		"report resource scaling result", "put", "/resourceScalingResult", targetNode, result,
		0, 10,
	)
	if err != nil {
		return false
	}
	return true
}

func (j *JADE) CommScaleResource(targetNode *ds.Node, action *rm.ScalingAction) bool {

	_, _, _, err := j.HTTPCommunicate(
		"scale resource", "put", "/scaleResource", targetNode, action,
		0, 10,
	)
	if err != nil {
		return false
	}
	return true
}

func (j *JADE) CommLocalResourceManagerAddon(port int, method string, path string, payload interface{}, response interface{}) error {
	if j.Config == nil || j.Config.SelfNode == nil || j.Config.SelfNode.IsAddrEmpty() {
		return errors.New("JADE is not ready to communicate yet")
	}
	node := &ds.Node{
		Addr:     j.Config.SelfNode.Addr,
		Protocol: j.Config.SelfNode.Protocol,
		Port:     port,
	}
	_, _, content, err := j.HTTPCommunicate(
		"communicating with local resource manager", method, path, node, payload,
		0, 10,
	)

	if err != nil {
		return err
	}

	if response != nil {
		if len(content) == 0 {
			return errors.New("JSON payload is empty")
		}
		err = json.Unmarshal(content, response)
		if err != nil {
			return err
		}
	}

	return nil
}
