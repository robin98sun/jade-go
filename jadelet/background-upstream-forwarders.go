package jadelet

func (j *JADE) feedbackTaskStatus(evalRes *TaskEvalResult) {
	if j.HasUpperNode() {
		payload := j.GeneratePayloadOfRequest(nil, evalRes, nil, nil)
		go j.HTTPCommunicate("feedback task acceptances", "POST", "/$jade$/feedbackAcceptances", j.Config.UpperNode, payload, 0, 10)
	} else {
		// send the result to UI
	}
}
