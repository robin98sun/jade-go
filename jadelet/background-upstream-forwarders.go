package jadelet

import (
	"encoding/json"
)

func (j *JADE) feedbackTaskAcceptances(evalRes *TaskEvalResult) {
	if j.HasUpperNode() {
		payload := j.GeneratePayloadOfRequest(nil, evalRes, nil, nil)
		res, err := j.HTTPCommunicate("feedback task acceptances", "POST", "/$jade$/collectAcceptances", j.Config.UpperNode, payload, 0, 10)
		if err != nil {
			j.log.Println("ERROR when feedback task acceptances:", err.Error())
		} else {
			resbytes, _ := json.MarshalIndent(res, "", "    ")
			j.log.Println("Response from of collecter of task acceptance:", string(resbytes))
		}
	} else {
		// send the result to UI
	}
}
