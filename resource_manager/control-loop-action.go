package resource_manager

import (
	// "uta.edu/aces/jade-go/perfstat"
	// "sync"
	// "math"
	// "strconv"
	// ds "uta.edu/aces/jadesdk/data_structure"
)


func (l *ControlLoop) updateLocalCPUResourceCache() {
	// refresh local resource cache from local resource manager
	// update local resource cache, regardless whether succeeded or not
	if l.MessengerCommLocalResourceManagerAddon != nil {

		type CPUResourcesOfPodsResponse struct {
			Error interface{} `json:"error,omitempty"`
			Pods  map[string]*CPUResourceItem `json:"pods,omitempty"`
		}

		resInst, err := (*l.MessengerCommLocalResourceManagerAddon)(
						l.LocalResourceManagerPort,
						"GET", "/kube-all-pods-cpu-resources",
						nil,
					)
		if err == nil{
			res := resInst.(*CPUResourcesOfPodsResponse)
			if res.Error == nil {
				l.CPUResourceCache.UpdatePods(res.Pods)
			}
		}
	}
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

	l.updateLocalCPUResourceCache()

	if action.ActionType == ScalingActionTypeUp || action.ActionType == ScalingActionTypeDown {

		deltaCores := float64(0)
		if action.ActionType == ScalingActionTypeUp {
			deltaCores = l.DefaultUnitForVerticalScaling
		} else if action.ActionType == ScalingActionTypeDown {
			deltaCores = -l.DefaultUnitForVerticalScaling
		}

		remainingCPUCores := l.CPUResourceCache.GetRemainingCPUCores()

		for i:=0; i<len(action.PodUIDs) && remainingCPUCores > 0; i++ {
			podKey := action.PodUIDs[i]
			resourceItem := l.CPUResourceCache.GetCPUResourceItem(podKey)
			currentCores := resourceItem.GetNormalizedCPUCores()
			targetCores := currentCores + deltaCores
			targetQuota, deltaQuota, maxCores := l.CPUResourceCache.CalcQuotaForTargetCPUCores(podKey, targetCores)
			if targetCores <= maxCores && targetQuota > 0 && deltaQuota != 0 {
				// actually take the action

				type CPUResourceUpdateResponse struct {
					Error interface{} `json:"error,omitempty"`
					Value int `json:"value,omitempty"`
				}

				resInst, err := (*l.MessengerCommLocalResourceManagerAddon)(
					l.LocalResourceManagerPort,
					"PUT", "/kube-pod-cpu-resource",
					map[string]interface{}{
						"type": "quota",
						"is_besteffort": false,
						"uid": podKey,
						"value": targetQuota,
					},
				)
				if err == nil{
					res := resInst.(*CPUResourceUpdateResponse)
					if res.Error == nil {
						result.Succeeded = true
						l.updateLocalCPUResourceCache()
					}
				}

			}
		}
	}


	if l.MessengerReportScalingResult != nil {
		(*l.MessengerReportScalingResult)(action.SourceNode, result)
	}

	l.mutex.Unlock()
}