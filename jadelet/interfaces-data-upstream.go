package jadelet

import (
	// "encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/scheduler"
	"uta.edu/aces/jadesdk"
	"strconv"
	"time"
)

func (j *JADE) CollectAppMsg(w rest.ResponseWriter, r *rest.Request) {
	timestampReceving := time.Now()
	msg := &jadesdk.ReportMessage{}
	err := r.DecodeJsonPayload(msg)
	retryCountStr := r.Header.Get("retry-count")
	var retryCount int64 = 0
	if retryCountStr != "" {
		retryCount, _ = strconv.ParseInt(retryCountStr, 10, 64)
	}
	if err == nil {

		// bs, _ := json.MarshalIndent(msg, "", "    ")
		// j.log.Println("[app message collector] Received application message:", string(bs))
		j.log.Op.Printf("[app message collector] Received application message which claims for subtask[%v] of task[%v], from pod[%v]:",
			msg.SubtaskKey, msg.TaskKey,
			msg.Node.Key(),
		)
		if msg.TaskKey != "" && msg.SubtaskKey != "" {
			if msg.Status == scheduler.TaskStatusFailed {
				j.TaskCache.FailTask(msg.TaskKey)
			}
			j.log.Op.Printf("[app message collector] processing result for subtask[%v] of task[%v] claimed by pod{%v}",
				msg.SubtaskKey, msg.TaskKey,
				msg.Node.Key(),
			)
			// save result and stat
			subtask, serviceRequestTime, communicationTime, queueingTime, budget := j.TaskCache.SaveResultFromApp(msg.TaskKey, msg.SubtaskKey, scheduler.TaskStatus(msg.Status), msg, retryCount, timestampReceving)
			if subtask != nil && subtask.Pod != nil {
				j.DoneRequest(w, r, "message received")
				j.log.Op.Printf("[app message collector] verified message for subtask[%v] of task[%v] from pod[%v]", subtask.GetKey(), subtask.TaskKey, msg.Node.Key())
				// then dequeue or release the pod queue
				j.PodCache.SetPodIdle(subtask.Pod, serviceRequestTime, communicationTime, queueingTime, budget)
				// forward aggregator subtask to upper tier if possible
				if subtask.ModuleName == string(kernel.AppModuleAggregator) && j.HasUpperNode() {
					j.sdk.SendReportMessageToJadelet(j.Config.UpperNode.GetSDKNode(), msg)
				}

				postQueryPerfAnalysis := func() {
					// to see if the task is done
					isTaskDone := j.TaskCache.CheckTask(msg.TaskKey, scheduler.TaskStatusDone, timestampReceving , j.log.Debug.Printf)

					if isTaskDone {
						// the query (task) is done
						j.log.Op.Printf("[app message collector] task[%v] is {%v}", msg.TaskKey, scheduler.TaskStatusDone)
						dispatchItem, unloaded_tail_latency, queueing_budget := j.TaskCache.GetDispatchingItem(msg.TaskKey)
						subtasks, pods := j.TaskCache.GetSubtasksPerNodeForTask(msg.TaskKey, "", "")

						percentile := dispatchItem.GetPercentile()
						adjusted_tail_latency := j.PodCache.CalcTailForPods(pods, percentile, scheduler.PodQueueHistogramTypeAdjustedServiceResponseTime)
						j.PerfCache.EnqueueResponse(dispatchItem, unloaded_tail_latency, queueing_budget, adjusted_tail_latency, timestampReceving, subtasks)
					} else {
						j.log.Op.Printf("[app message collector] task[%v] is NOT {%v} yet", msg.TaskKey, scheduler.TaskStatusDone)
					}
				}

				postQueryPerfAnalysis()	
				
				return
			}
		}
		j.log.Op.Printf("[app message collector] ERROR: the subtask[%v] of task[%v] claimed by a message from pod[%v] is not recognized",
			msg.SubtaskKey, msg.TaskKey,
			msg.Node.Key(),
		)
		j.PeacefulFatalRequest(w, r, "invalid subtask")

	} else {
		j.PeacefulFatalRequest(w, r, "invalid message: "+err.Error())
	}
}

// func (j *JADE) DumpStat(w rest.ResponseWriter, r *rest.Request) {
// 	if j.TaskCache != nil {
// 		j.DoneRequest(w, r, j.TaskCache.Stat)
// 	} else {
// 		j.PeacefulFatalRequest(w, r, "Task cache is not available")
// 	}
// }

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
