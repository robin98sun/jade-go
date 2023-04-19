package jadelet

import (
	"encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	// "uta.edu/aces/jade-go/scheduler"
	// "uta.edu/aces/jade-go/kernel"
	ds "uta.edu/aces/jadesdk/data_structure"
)

type TaskReceiverResponse struct {
	DataPlaneTasksCount int  `json:"dataPlaneTasks,omitempty"`
	ControlPlaneTasksCount int  `json:"controlPlaneTasks,omitempty"`
	TaskIDList      []string `json:"taskIDList,omitempty"`
	DiscoveryTime   float64 `json:"discoveryTime,omitempty"`
	NegotiationTime float64 `json:"negotiationTime,omitempty"`
	MatchTime       float64 `json:"matchTime,omitempty"`
	PopulateTime    float64 `json:"populateTime,omitempty"`
	NeighborCount   int     `json:"neighbors,omitempty"`
	PackageSize     int     `json:"packageSize,omitempty"`
	StrugglingNodes float64     `json:"struggling_nodes,omitempty"`
}

// TaskReceiver task receiver
func (j *JADE) TaskReceiver(w rest.ResponseWriter, r *rest.Request) {
	content, _, err := j.ValidateRequest(w, r)
	if err != nil {
		j.PeacefulFatalRequest(w, r, err.Error())
		return
	}
	// re-decode
	reqInst := &struct {
		Payload []*ds.TaskDispatchingItem `json:"payload,omitempty"`
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

		dataPlaneTasks := make(map[string]*ds.TaskDispatchingItem)
		controlPlaneTasks := map[string]*ds.TaskDispatchingItem{}

		res := &TaskReceiverResponse{}
		
		for _, taskItem := range taskList {
			if taskItem.Task != nil && taskItem.Task.Valid() {
				taskItem.Arrived()
				taskItem.GenTag()
				if taskItem.Options != nil && taskItem.Options.IsControlPlaneTask && taskItem.Options.ControlPlaneOptions != nil {

					controlPlaneTasks[taskItem.Task.GetKey()] = taskItem					
				} else {

					dataPlaneTasks[taskItem.Task.GetKey()] = taskItem
				}
				res.TaskIDList = append(res.TaskIDList, taskItem.Task.GetKey())
			} else {
				j.log.Op.Println("WARN: received an invalid task")
				res.TaskIDList = append(res.TaskIDList, "")
			}
		}
		j.log.Op.Printf("received %v data plane tasks, %v control plane tasks", len(dataPlaneTasks), len(controlPlaneTasks))
		if len(dataPlaneTasks) > 0 {
			j.ClassifyDataPlaneTasks(dataPlaneTasks)
		}

		if len(controlPlaneTasks) > 0 {
			avg_discovery_time := float64(0)
			avg_negotiation_time := float64(0)
			avg_neighbor_count := 0
			avg_match_time := float64(0)
			avg_populate_time := float64(0)
			avg_package_size := 0
			avg_struggling_nodes := float64(0)
			for _, taskItem := range controlPlaneTasks {
				neighborCount, total_time, discovery_time, matching_time, populating_time, package_size, struggling_nodes := j.processControlPlaneTask(taskItem)
				negotiation_time := total_time - discovery_time
				avg_discovery_time += discovery_time
				avg_negotiation_time += negotiation_time
				avg_neighbor_count += neighborCount
				avg_match_time += matching_time
				avg_populate_time += populating_time
				avg_package_size += package_size
				avg_struggling_nodes += float64(struggling_nodes)
			}
			avg_discovery_time /= float64(len(controlPlaneTasks))
			avg_negotiation_time /= float64(len(controlPlaneTasks))
			avg_neighbor_count /= len(controlPlaneTasks)
			avg_match_time /= float64(len(controlPlaneTasks))
			avg_populate_time /= float64(len(controlPlaneTasks))
			avg_package_size /= len(controlPlaneTasks)
			avg_struggling_nodes /= float64(len(controlPlaneTasks))
			res.DiscoveryTime = avg_discovery_time
			res.NegotiationTime = avg_negotiation_time
			res.NeighborCount = avg_neighbor_count
			res.MatchTime = avg_match_time
			res.PopulateTime = avg_populate_time
			res.PackageSize = avg_package_size
			res.StrugglingNodes = avg_struggling_nodes
		}

		res.DataPlaneTasksCount = len(dataPlaneTasks)
		res.ControlPlaneTasksCount = len(controlPlaneTasks)
		j.DoneRequest(w, r, res)
	}

}


func (j *JADE) ClassifyDataPlaneTasks(tasklist map[string]*ds.TaskDispatchingItem) {
	
	collaborativeTasks := map[string]*ds.TaskDispatchingItem{}
	aggregativeTasks := map[string]*ds.TaskDispatchingItem{}

	for taskKey, dispatchItem := range tasklist {
		resultBytes, _ := json.MarshalIndent(dispatchItem.Options, "", "  ")
		j.log.Debug.Printf("received task option:", string(resultBytes))
		if j.HasRegistry() && dispatchItem.TTL > 0 {
			j.log.Op.Printf("received a collaborative task [%v], ttl: %v", taskKey, dispatchItem.TTL)
			collaborativeTasks[taskKey] = dispatchItem
		} else {
			j.log.Op.Printf("received an autonomous task [%v], ttl: %v", taskKey, dispatchItem.TTL)
			task := dispatchItem.Task
			if _, aggregatorExists := task.Application.Modules[string(ds.AppModuleAggregator)]; aggregatorExists {
				if _, workerExists := task.Application.Modules[string(ds.AppModuleWorker)]; workerExists {
					aggregativeTasks[taskKey] = dispatchItem
					j.log.Op.Printf("the autonomous task is an aggregative task")
					j.PerfCache.EnqueueArrivalTime(dispatchItem.Task.Application.Key(), dispatchItem, dispatchItem.ArriveTimestamp)
				}
			}
		}
	}
	if len(aggregativeTasks) > 0 {
		j.log.Op.Printf("evaluating %v aggregative tasks", len(aggregativeTasks))
		go j.evaluateAggregativeTasks(aggregativeTasks)
	}
	if len(collaborativeTasks) > 0 {
		j.log.Op.Printf("evaluating %v collaborative tasks", len(aggregativeTasks))
		go j.evaluateCollaborativeTasks(collaborativeTasks)
	}
}
