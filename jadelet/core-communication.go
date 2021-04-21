package jadelet

import (
	"uta.edu/aces/jade-go/kernel"
	"time"
)

func (j *JADE) HTTPCommunicate(
	operationName string, method string, path string, targetNode *kernel.Node,
	payload interface{}, retryCnt int, retryLimitation int,
) (interface{}, int, time.Time, time.Duration, error) {
	return j.sdk.HTTPCommunicate(
		operationName, targetNode.Protocol, method, path,
		targetNode.GetSDKNode(), payload, retryCnt, retryLimitation, time.Time{},
	)
}
