package jadelet

import (
	"encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jade-go/kernel"
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
				taskItem.Arrived()
				validTasks[taskItem.Task.GetKey()] = taskItem
				res.TaskIDList = append(res.TaskIDList, taskItem.Task.GetKey())
			} else {
				j.log.Println("WARN: received an invalid task")
				res.TaskIDList = append(res.TaskIDList, "")
			}
		}
		if len(validTasks) > 0 {
			j.ClassifyTasks(validTasks)
		}

		res.ValidTasksCount = len(validTasks)
		j.DoneRequest(w, r, res)
	}

}


func (j *JADE) ClassifyTasks(tasklist map[string]*scheduler.TaskDispatchingItem) {
	collaborativeTasks := map[string]*scheduler.TaskDispatchingItem{}
	aggregativeTasks := map[string]*scheduler.TaskDispatchingItem{}
	for taskKey, dispatchItem := range tasklist {
		if j.HasRegistry() && dispatchItem.TTL > 0 {
			collaborativeTasks[taskKey] = dispatchItem
		} else {
			task := dispatchItem.Task
			if _, aggregatorExists := task.Application.Modules[string(kernel.AppModuleAggregator)]; aggregatorExists {
				if _, workerExists := task.Application.Modules[kernel.AppModuleWorker]; workerExists {
					aggregativeTasks[taskKey] = dispatchItem
				}
			}
		}
	}
	if len(aggregativeTasks) > 0 {
		go j.evaluateAggregativeTasks(aggregativeTasks)
	}
	if len(collaborativeTasks) > 0 {
		go j.evaluateCollaborativeTasks(collaborativeTasks)
	}
}
