package resource_manager

import (
	// "uta.edu/aces/jade-go/perfstat"
	// "sync"
	"math"
	// "strconv"
	// ds "uta.edu/aces/jadesdk/data_structure"
	"encoding/json"
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
	IsBesteffort bool `json:"is_besteffort,omitempty"`
	UID string `json:"uid,omitempty"`
	Value int `json:"value,omitempty"`
}


func (l *ControlLoop) InitPodCPUResource(podUID string, cpuCores float64, printf func(template string, args ...interface{})) {

	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.MessengerCommLocalResourceManagerAddon == nil {
		printf("[resource manager] ERROR: messenger for communicating local resource manager addon is nil")
		return
	}


	l.updateLocalCPUResourceCache(printf)
	shares := l.CPUResourceCache.TotalShares
	period := l.CPUResourceCache.GetPodResource(CPUResourceTypePeriod, podUID)
	if period <= 0 || shares <= 0 {
		printf("[resource manager] ERROR: total shares=%v, for pod UID=%v period=%v", shares, podUID, period)
		return
	}

	quota := int(math.Round(cpuCores * float64(period)))

	_, content1, err1 := (*l.MessengerCommLocalResourceManagerAddon)(
		l.LocalResourceManagerPort,
		"PUT", "/kube-pod-cpu-resource",
		&CPUResourceUpdateRequest{
			Type: "quota",
			IsBesteffort: false,
			UID: podUID,
			Value: quota,
		},
	)

	_, content2, err2 := (*l.MessengerCommLocalResourceManagerAddon)(
		l.LocalResourceManagerPort,
		"PUT", "/kube-pod-cpu-resource",
		&CPUResourceUpdateRequest{
			Type: "shares",
			IsBesteffort: false,
			UID: podUID,
			Value: shares,
		},
	)

	res1 := &CPUResourceUpdateResponse{}
	res2 := &CPUResourceUpdateResponse{}

	err11 := json.Unmarshal(content1, res1)
	err22 := json.Unmarshal(content2, res2)

	if err1 != nil || err11 != nil || res1.Error != nil {
		printf("[resource manager] ERROR when updating quota to %v for pod UID=%v, comm error: %v, decoding error:%v, service error: %v", quota, podUID, err1, err11, res1.Error)
	} else {
		printf("[resource manager] pod UID=%v, quota has been updated to %v", podUID, res1.Value)
	}

	if err2 != nil || err22 != nil || res2.Error != nil  {
		printf("[resource manager] ERROR when updating shares to %v for pod UID=%v, comm error: %v, decoding error %v, service error: %v", shares, podUID, err2, err22, res2.Error)
	} else {
		printf("[resource manager] pod UID=%v, shares has been updated to %v", podUID, res2.Value)
	}

}


func (l *ControlLoop) updateLocalCPUResourceCache(printf func(template string, args ...interface{})) {
	// refresh local resource cache from local resource manager
	// update local resource cache, regardless whether succeeded or not
	if l.MessengerCommLocalResourceManagerAddon != nil {

		resInst1, content, err := (*l.MessengerCommLocalResourceManagerAddon)(
						l.LocalResourceManagerPort,
						"GET", "/kube-all-pods-cpu-resources",
						nil,
					)
		if err != nil {
			if printf != nil {
				printf("[resource manager] ERROR when querying cpu resources of all pods, error: %v", err)
			}
		} else {
			res := &CPUResourcesOfPodsResponse{}
			err2 := json.Unmarshal(content, res)
			if err2 != nil {
				if printf != nil {
					printf("[resource manager] ERROR when decoding response of querying cpu resourece of all pods, error: %v", err2)
				}
			} else if res.Error != nil {
				if printf != nil {
					printf("[resource manager] ERROR in the response of querying cpu resourece of all pods, error: %v", res.Error)
				}
			} else {
				l.CPUResourceCache.UpdatePods(res.Pods)
				if printf != nil {
					printf("[resource manager] got pods for resource cache: %v, original res:", res, resInst1)
				}
				resInst2, content_cores, err3 := (*l.MessengerCommLocalResourceManagerAddon)(
					l.LocalResourceManagerPort,
					"GET", "/cpu-cores",
					nil,
				)
				if err3 != nil {
					if printf != nil {
						printf("[resource manager] ERROR when querying cpu cores: error: %v", err3)
					}
				} else {
					res_cores := &CPUCoresQueryResponse{}
					err4 := json.Unmarshal(content_cores, res_cores)
					if err4 != nil {
						if printf != nil {
							printf("[resource manager] ERROR when decoding response of querying cpu cores, error: %v", err4)
						}
					} else if res_cores.Error != nil {
						if printf != nil {
							printf("[resource manager] ERROR in the response of querying cpu cores, error: %v", res_cores.Error)
						}
					} else {
						l.CPUResourceCache.SetCPUCores(res_cores.Cores)

						if printf != nil {
							printf("[resource manager] got cpu cores: %v, original res: %v", res_cores, resInst2)
						}

						resInst3, content_shares, err4 := (*l.MessengerCommLocalResourceManagerAddon)(
							l.LocalResourceManagerPort,
							"GET", "/kube-overall-cpu-shares",
							nil,
						)
						if err4 != nil {
							if printf != nil {
								printf("[resource manager] ERROR when querying cpu shares: error: %v", err4)
							}
						} else {
							res_shares := &OverallSharesQueryResponse{}
							err5 := json.Unmarshal(content_shares, res_shares)
							if err5 != nil {
								if printf != nil {
									printf("[resource manager] ERROR when decoding response of querying overall shares, error: %v", err5)
								}
							} else if res_shares.Error != nil {
								if printf != nil {
									printf("[resource manager] ERROR in the response of querying overall shares, error: %v", res_shares.Error)
								}
							} else {
								l.CPUResourceCache.SetTotalShares(res_shares.Shares)
								if printf != nil {
									printf("[resource manager] got overall shares: %v, original res: %v", res_shares, resInst3)
								}
							}
						}
					}
				}
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

	l.updateLocalCPUResourceCache(nil)

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

				
				_, content, err := (*l.MessengerCommLocalResourceManagerAddon)(
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
					res := &CPUResourceUpdateResponse{}
					err2 := json.Unmarshal(content, res)
					if res != nil && res.Error == nil && err2 == nil{
						result.Succeeded = true
						l.updateLocalCPUResourceCache(nil)
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