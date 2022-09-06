package jadelet

import (
	"uta.edu/aces/jade-go/kernel"
	ds "uta.edu/aces/jadesdk/data_structure"
)

func (j *JADE) HTTPCommunicate(
	operationName string, method string, path string, targetNode *ds.Node,
	payload interface{}, retryCnt int, retryLimitation int,
) (interface{}, int, []byte, error) {
	return j.sdk.HTTPCommunicate(
		operationName, targetNode.Protocol, method, path,
		targetNode.GetSDKNode(), payload, retryCnt, retryLimitation,
	)
}
