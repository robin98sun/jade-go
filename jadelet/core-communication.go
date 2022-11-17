package jadelet

import (
<<<<<<< HEAD
	// "uta.edu/aces/jade-go/kernel"
=======
>>>>>>> refactoring
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
