package jadelet

import (
	ds "uta.edu/aces/jadesdk/data_structure"
	rm "uta.edu/aces/jade-go/resource_manager"
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
