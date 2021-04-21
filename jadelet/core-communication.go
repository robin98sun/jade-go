package jadelet

import (
	"uta.edu/aces/jade-go/kernel"
	"time"
)

func (j *JADE) HTTPCommunicate(
	operationName string, method string, path string, targetNode *kernel.Node,
	payload interface{}, retryCnt int, retryLimitation int,
) (interface{}, int, time.Time, time.Duration, error) {
	// tsStart := time.Now()
	res, bytes, timestamp, dur, err := j.sdk.HTTPCommunicate(
		operationName, targetNode.Protocol, method, path,
		targetNode.GetSDKNode(), payload, retryCnt, retryLimitation, time.Time{},
	)
	// tsEnd := time.Now()
	// if timestamp.IsZero() {
	// 	timestamp = tsStart
	// 	dur = tsEnd.Sub(tsStart)
	// }
	return res, bytes, timestamp, dur, err
}
