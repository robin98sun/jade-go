package jadelet

import (
	"encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	"uta.edu/aces/jade-go/scheduler"
)

// TaskReceiver task receiver
func (j *JADE) TaskReceiver(w rest.ResponseWriter, r *rest.Request) {
	content, _, err := j.ValidateRequest(w, r)
	if err != nil {
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}
	// re-decode
	reqInst := &struct {
		Payload []*scheduler.TaskDispatchingItem `json:"payload,omitempty"`
	}{}
	err = json.Unmarshal(content, reqInst)
	if err != nil {
		j.PeacefulFatalRequest(w, r, "can not decode task list: "+err.Error())
		return
	} else {
		if reqInst == nil || reqInst.Payload == nil || len(reqInst.Payload) == 0 {
			j.PeacefulFatalRequest(w, r, "task receiver received empty payload")
			return
		}
		taskList := reqInst.Payload
		validTasks := make(map[string]*scheduler.TaskDispatchingItem)
		res := &struct {
			ValidTasksCount int      `json:"validTasksCount,omitempty"`
			TaskIDList      []string `json:"taskIDList,omitempty"`
		}{}
		for _, taskItem := range taskList {
			if taskItem.Task != nil && taskItem.Task.Valid() {
				validTasks[taskItem.Task.GetKey()] = taskItem
				res.TaskIDList = append(res.TaskIDList, taskItem.Task.GetKey())
			} else {
				j.log.Println("WARN: received an invalid task")
				res.TaskIDList = append(res.TaskIDList, "")
			}
		}
		if len(validTasks) > 0 {
			go j.evaluateTasks(validTasks)
		}

		res.ValidTasksCount = len(validTasks)
		j.DoneRequest(w, r, res)
	}

}
