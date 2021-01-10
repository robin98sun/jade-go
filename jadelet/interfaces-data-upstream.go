package jadelet

import (
	// "encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jadesdk"
)

func (j *JADE) CollectAppMsg(w rest.ResponseWriter, r *rest.Request) {
	msg := &jadesdk.ReportMessage{}
	err := r.DecodeJsonPayload(msg)
	if err == nil {
		// bs, _ := json.MarshalIndent(msg, "", "    ")
		// j.log.Println("[app message collector] Received application message:", string(bs))
		j.log.Printf("[app message collector] Received application message which claims for subtask[%v] of task[%v], from pod[%v]:",
			msg.SubtaskKey, msg.TaskKey,
			msg.Node.Key(),
		)
		if msg.TaskKey != "" && msg.SubtaskKey != "" {
			if msg.Status == scheduler.TaskStatusFailed {
				j.TaskCache.FailTask(msg.TaskKey)
			}
			j.log.Printf("[app message collector] processing result for subtask[%v] of task[%v] claimed by pod{%v}",
				msg.SubtaskKey, msg.TaskKey,
				msg.Node.Key(),
			)
			subtask := j.TaskCache.SaveResultFromApp(msg.TaskKey, msg.SubtaskKey, scheduler.TaskStatus(msg.Status), msg.Updates, msg.Stat)
			if subtask != nil && subtask.Pod != nil {
				j.log.Printf("[app message collector] verified message for subtask[%v] of task[%v] from pod[%v]", subtask.GetKey(), subtask.TaskKey, msg.Node.Key())
				j.DoneRequest(w, r, "message received")
				// then dequeue or release the pod queue
				j.PodCache.SetPodIdle(subtask.Pod)
				// to see if the task is done
				j.log.Printf("[app message collector] checking if task[%v] is {%v}", msg.TaskKey, scheduler.TaskStatusDone)
				j.TaskCache.CheckTask(msg.TaskKey, scheduler.TaskStatusDone, j.log.Printf)
				return
			}
		}
		j.log.Printf("[app message collector] ERROR: the subtask[%v] of task[%v] claimed by a message from pod[%v] is not recognized",
			msg.SubtaskKey, msg.TaskKey,
			msg.Node.Key(),
		)
		j.PeacefulFatalRequest(w, r, "invalid subtask")
	} else {
		j.PeacefulFatalRequest(w, r, "invalid message: "+err.Error())
	}
}

func (j *JADE) DumpStat(w rest.ResponseWriter, r *rest.Request) {
	if j.TaskCache != nil {
		j.DoneRequest(w, r, j.TaskCache.Stat)
	} else {
		j.PeacefulFatalRequest(w, r, "Task cache is not available")
	}
}

func (j *JADE) GetAggregativeTaskResults(w rest.ResponseWriter, r *rest.Request) {
	if j.TaskCache == nil || len(j.TaskCache.Cache) == 0 {
		j.PeacefulFatalRequest(w, r, "Task cache is empty")
		return
	}
	taskIDList := []string{}
	for key, value := range r.URL.Query() {
		if key == "tasks" {
			for _, taskId := range value {
				taskIDList = append(taskIDList, taskId)
			}
		}
	}
	if len(taskIDList) == 0 {
		j.PeacefulFatalRequest(w, r, "empty request")
	} else {
		results := make(map[string][]*scheduler.TaskResult)
		for _, taskKey := range taskIDList {
			taskResult := j.TaskCache.GetResultOfTask(taskKey, string(kernel.AppModuleAggregator))
			results[taskKey] = taskResult
		}
		j.DoneRequest(w, r, results)
	}
}
