package jadelet

import (
	"encoding/json"
	"uta.edu/aces/jade-go/kernel"
)

type TaskProvisioningResult struct {
	NodeKey    string      `json:"nodeID,omitempty"`
	TaskKey    string      `json:"taskID,omitempty"`
	ModuleName string      `json:"moduleName,omitempty"`
	Pod        *kernel.Pod `json:"pod,omitempty"`
}

func NewTaskProvisioningResult(nodekey string, taskkey string, moduleName string, pod *kernel.Pod) *TaskProvisioningResult {
	inst := &TaskProvisioningResult{
		NodeKey:    nodekey,
		TaskKey:    taskkey,
		ModuleName: moduleName,
		Pod:        pod,
	}
	return inst
}

func (j *JADE) feedbackProvisioning(result *TaskProvisioningResult) {
	if j.HasUpperNode() {
		payload := j.GeneratePayloadOfRequest(nil, result, nil, nil)
		res, err := j.HTTPCommunicate("feedback task provisioning", "POST", "/$jade$/collectProvisioning", j.Config.UpperNode, payload, 0, 10)
		if err != nil {
			j.log.Println("ERROR when feedback task provisioning:", err.Error())
		} else {
			resbytes, _ := json.MarshalIndent(res, "", "    ")
			j.log.Println("Response from of collecter of task provisioning:", string(resbytes))
		}
	} else {
		// send the result to UI
	}
}
