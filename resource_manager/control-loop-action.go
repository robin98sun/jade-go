package resource_manager

import (
	// "uta.edu/aces/jade-go/perfstat"
	// "sync"
	"math"
	// "strconv"
	// ds "uta.edu/aces/jadesdk/data_structure"
	// "encoding/json"
)

type CPUResourcesOfPodsResponse struct {
	Error interface{} `json:"error,omitempty"`
	Pods  map[string]*CPUResourceItem `json:"pods,omitempty"`
}

type CPUResourceUpdateResponse struct {
	Error interface{} `json:"error,omitempty"`
	Value int `json:"value,omitempty"`
}

type CPUCoresQueryResponse struct {
	Error interface{} `json:"error,omitempty"`
	Cores int `json:"cores,omitempty"`
}

type OverallSharesQueryResponse struct {
	Error interface{} `json:"error,omitempty"`
	Shares int `json:"shares,omitempty"`
}

type CPUResourceUpdateRequest struct {
	Type string `json:"type,omitempty"`
	IsBesteffort bool `json:"is_besteffort"`
	UID string `json:"uid,omitempty"`
	Value int `json:"value,omitempty"`
}


func (l *ControlLoop) updateLocalCPUResourceCache(printf func(template string, args ...interface{})) {
	// refresh local resource cache from local resource manager
	// update local resource cache, regardless whether succeeded or not
	if l.MessengerCommLocalResourceManagerAddon != nil {

		responsePods := &CPUResourcesOfPodsResponse{}
		err := (*l.MessengerCommLocalResourceManagerAddon)(
						l.LocalResourceManagerPort,
						"GET", "/kube-all-pods-cpu-resources",
						nil, responsePods,
					)
		if err != nil {
			if printf != nil {
				printf("[resource manager] ERROR when querying cpu resources of all pods, error: %v", err)
			}
		} else {
			l.CPUResourceCache.UpdatePods(responsePods.Pods)
			if printf != nil {
				printf("[resource manager] got pods for resource cache original res:", responsePods)
			}
		}

		responseCores := &CPUCoresQueryResponse{}
		err3 := (*l.MessengerCommLocalResourceManagerAddon)(
			l.LocalResourceManagerPort,
			"GET", "/cpu-cores",
			nil, responseCores,
		)
		if err3 != nil {
			if printf != nil {
				printf("[resource manager] ERROR when querying cpu cores: error: %v", err3)
			}
		} else {
			l.CPUResourceCache.SetCPUCores(responseCores.Cores)
				
			if printf != nil {
				printf("[resource manager] got cpu cores original res: %v", responseCores)
			}
		}


		responseShares := &OverallSharesQueryResponse{}
		err4 := (*l.MessengerCommLocalResourceManagerAddon)(
			l.LocalResourceManagerPort,
			"GET", "/kube-overall-cpu-shares",
			nil, responseShares,
		)
		if err4 != nil {
			if printf != nil {
				printf("[resource manager] ERROR when querying cpu shares: error: %v", err4)
			}
		} else {
			l.CPUResourceCache.SetTotalShares(responseShares.Shares)
			if printf != nil {
				printf("[resource manager] got overall shares original res: %v", responseShares)
			}
		}
	}
}


func (l *ControlLoop) SetPodCPUResource(podUID string, cpuCores float64, printf func(template string, args ...interface{})) {

	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.MessengerCommLocalResourceManagerAddon == nil {
		printf("[resource manager] ERROR: messenger for communicating local resource manager addon is nil")
		return
	}


	l.updateLocalCPUResourceCache(printf)
	shares := int(math.Round(float64(l.CPUResourceCache.TotalShares) * cpuCores / float64(l.CPUResourceCache.CPUCores)))
	period := l.CPUResourceCache.GetPodResource(CPUResourceTypePeriod, podUID)
	if period <= 0 || shares <= 0 {
		printf("[resource manager] ERROR: total shares=%v, for pod UID=%v period=%v", shares, podUID, period)
		return
	}

	quota := int(math.Round(cpuCores * float64(period)))

	(*l.MessengerCommLocalResourceManagerAddon)(
		l.LocalResourceManagerPort,
		"PUT", "/kube-pod-cpu-resource",
		&CPUResourceUpdateRequest{
			Type: "quota",
			IsBesteffort: false,
			UID: podUID,
			Value: quota,
		}, nil,
	)

	(*l.MessengerCommLocalResourceManagerAddon)(
		l.LocalResourceManagerPort,
		"PUT", "/kube-pod-cpu-resource",
		&CPUResourceUpdateRequest{
			Type: "shares",
			IsBesteffort: false,
			UID: podUID,
			Value: shares,
		}, nil,
	)
}


func (l *ControlLoop) PhysicallyExecuteAction(action *ScalingAction) {

	// the master node sent this action to this node, regardless whether this node have resources or not

	// read available resources from the resource manager
	if action == nil || len(action.PodUIDs) == 0 {
		return 
	}
	if action.SourceNode == nil {return}


	// report the result

	l.mutex.Lock()

	result := &ScalingResult{
		ActionKey: action.GetKey(),
		Succeeded: false,
	}

	l.updateLocalCPUResourceCache(nil)

	if action.ActionType == ScalingActionTypeUp || action.ActionType == ScalingActionTypeDown {

		// deltaCores := float64(0)
		// if action.ActionType == ScalingActionTypeUp {
		// 	deltaCores = l.DefaultUnitForVerticalScaling
		// } else if action.ActionType == ScalingActionTypeDown {
		// 	deltaCores = -l.DefaultUnitForVerticalScaling
		// }

		// remainingCPUCores := l.CPUResourceCache.GetRemainingCPUCores()

		// for i:=0; i<len(action.PodUIDs) && remainingCPUCores > 0; i++ {
		// 	podKey := action.PodUIDs[i]
		// 	resourceItem := l.CPUResourceCache.GetCPUResourceItem(podKey)
		// 	currentCores := resourceItem.GetNormalizedCPUCores()
		// 	targetCores := currentCores + deltaCores
		// 	targetQuota, deltaQuota, maxCores := l.CPUResourceCache.CalcQuotaForTargetCPUCores(podKey, targetCores)
		// 	if targetCores <= maxCores && targetQuota > 0 && deltaQuota != 0 {
		// 		// actually take the action

				
		// 		_, content, err := (*l.MessengerCommLocalResourceManagerAddon)(
		// 			l.LocalResourceManagerPort,
		// 			"PUT", "/kube-pod-cpu-resource",
		// 			map[string]interface{}{
		// 				"type": "quota",
		// 				"is_besteffort": false,
		// 				"uid": podKey,
		// 				"value": targetQuota,
		// 			},
		// 		)
		// 		if err == nil{
		// 			res := &CPUResourceUpdateResponse{}
		// 			err2 := json.Unmarshal(content, res)
		// 			if res != nil && res.Error == nil && err2 == nil{
		// 				result.Succeeded = true
		// 				l.updateLocalCPUResourceCache(nil)
		// 			}
		// 		}

		// 	}
		// }
	}


	if l.MessengerReportScalingResult != nil {
		(*l.MessengerReportScalingResult)(action.SourceNode, result)
	}

	l.mutex.Unlock()
}