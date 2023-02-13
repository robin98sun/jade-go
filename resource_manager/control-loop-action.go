package resource_manager

import (
	// "uta.edu/aces/jade-go/perfstat"
	// "sync"
	"math"
	// "strconv"
	// ds "uta.edu/aces/jadesdk/data_structure"
)

type CPUResourcesOfPodsResponse struct {
	Error interface{} `json:"error,omitempty"`
	Pods  map[string]*CPUResourceItem `json:"pods,omitempty"`
}

type CPUResourceUpdateResponse struct {
	Error interface{} `json:"error,omitempty"`
	Value int `json:"value,omitempty"`
}


func (l *ControlLoop) InitPodCPUResource(podUID string, cpuCores float64, printf func(template string, args ...interface{})) {

	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.MessengerCommLocalResourceManagerAddon == nil {
		printf("[resource manager] ERROR: messenger for communicating local resource manager addon is nil")
		return
	}


	l.updateLocalCPUResourceCache()
	shares := l.CPUResourceCache.TotalShares
	period := l.CPUResourceCache.GetPodResource(CPUResourceTypePeriod, podUID)
	if period <= 0 || shares <= 0 {
		printf("[resource manager] ERROR: total shares=%v, for pod UID=%v period=%v", shares, podUID, period)
		return
	}

	quota := int(math.Round(cpuCores * float64(period)))

	resInst1, err1 := (*l.MessengerCommLocalResourceManagerAddon)(
		l.LocalResourceManagerPort,
		"PUT", "/kube-pod-cpu-resource",
		map[string]string{
			"type": "quota",
			"is_besteffort": "N",
			"uid": podUID,
			"value": quota,
		},
	)

	resInst2, err2 := (*l.MessengerCommLocalResourceManagerAddon)(
		l.LocalResourceManagerPort,
		"PUT", "/kube-pod-cpu-resource",
		map[string]string{
			"type": "shares",
			"is_besteffort": "N",
			"uid": podUID,
			"value": shares,
		},
	)

	res1 := resInst1.(*CPUResourceUpdateResponse)
	res2 := resInst2.(*CPUResourceUpdateResponse)
	if err1 != nil || res1.Error != nil {
		printf("[resource manager] ERROR when updating quota to %v for pod UID=%v, comm error: %v, service error: %v", quota, podUID, err1, res1.Error)
	} else {
		printf("[resource manager] pod UID=%v, quota has been updated to %v", podUID, res1.Value)
	}

	if res2.Error != nil {
		printf("[resource manager] ERROR when updating shares to %v for pod UID=%v, comm error: %v, service error: %v", shares, podUID, err2, res2.Error)
	} else {
		printf("[resource manager] pod UID=%v, shares has been updated to %v", podUID, res2.Value)
	}

}


func (l *ControlLoop) updateLocalCPUResourceCache() {
	// refresh local resource cache from local resource manager
	// update local resource cache, regardless whether succeeded or not
	if l.MessengerCommLocalResourceManagerAddon != nil {

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