package resource_manager

import (
	"uta.edu/aces/jade-go/perfstat"
	"time"
	"math/rand"
)

func (l *ControlLoop) daemon() {	
	INTERVAL := 100
	if l.DaemonIntervalInMilliseconds > 0 {
		INTERVAL = l.DaemonIntervalInMilliseconds
	}
	for {
		time.Sleep(time.Duration(INTERVAL + rand.Intn(INTERVAL/20))*time.Millisecond)
		l.mutex.Lock()		
		if l.DaemonIntervalInMilliseconds > 0 && l.DaemonIntervalInMilliseconds != INTERVAL {
			INTERVAL = l.DaemonIntervalInMilliseconds
		}
		msgBuff := l.PerfMessageBuffer
		l.PerfMessageBuffer = []*perfstat.PerfMessage{}
		if !l.isSetup() || !l.EnableAutoScaling {
			l.mutex.Unlock()
			continue
		}
		thresholdAverageSLOViolationRatio := l.Parameters.ThresholdAverageSLOViolationRatio
		thresholdAverageSLOSurplusRatio := l.Parameters.ThresholdAverageSLOSurplusRatio
		l.mutex.Unlock()

		// so long as guaranteeing single-thread-mode for this daemon
		// it is unnecessary to keep lock while evaluating the scaling plan
		scaleUpPlan := make(map[string]map[string]*perfstat.QueuePerfMessage) // nodeKey -> appKey -> msg
		scaleDownPlan := make(map[string]map[string]*perfstat.QueuePerfMessage) // nodeKey -> appKey -> msg
		
		for x, msg := range msgBuff {
			if msg == nil {
				for i := 0; i<100; i++ {
					msg = msgBuff[x]
					if msg != nil {
						break
					}
				}
				if msg == nil {
					continue
				}
			}
			if msg.Type == perfstat.PerfMessageTypeAvgTaskSLORatios && msg.AvgTaskSLORatios!=nil {
				taskSLORatios := msg.AvgTaskSLORatios
				if !l.isOutofCalmdownWindow(taskSLORatios.AppKey, taskSLORatios.Clock) {
					continue
				}
				if taskSLORatios.Violation >= thresholdAverageSLOViolationRatio {
					scaleUpCandidates := l.getScaleUpCandidates(taskSLORatios.AppKey, thresholdAverageSLOViolationRatio)
					if len(scaleUpCandidates) > 0 {
						for _, queuePerf := range scaleUpCandidates {
							if _, e := scaleUpPlan[queuePerf.QueueKey]; !e {
								scaleUpPlan[queuePerf.QueueKey] = make(map[string]*perfstat.QueuePerfMessage)
							}
							if _, e := scaleUpPlan[queuePerf.QueueKey][taskSLORatios.AppKey]; !e {
								scaleUpPlan[queuePerf.QueueKey][taskSLORatios.AppKey] = queuePerf
							} else if scaleUpPlan[queuePerf.QueueKey][taskSLORatios.AppKey].Clock < taskSLORatios.Clock {
								scaleUpPlan[queuePerf.QueueKey][taskSLORatios.AppKey] = queuePerf
							}
						}
					}
				} else if taskSLORatios.Surplus <= thresholdAverageSLOSurplusRatio {
					scaleDownCandidates := l.getScaleDownCandidates(taskSLORatios.AppKey, thresholdAverageSLOSurplusRatio)
					if len(scaleDownCandidates) > 0 {
						for _, queuePerf := range scaleDownCandidates {
							if _, e := scaleDownPlan[queuePerf.QueueKey]; !e {
								scaleDownPlan[queuePerf.QueueKey] = make(map[string]*perfstat.QueuePerfMessage)
							}
							if _, e := scaleDownPlan[queuePerf.QueueKey][taskSLORatios.AppKey]; !e {
								scaleDownPlan[queuePerf.QueueKey][taskSLORatios.AppKey] = queuePerf
							} else if scaleDownPlan[queuePerf.QueueKey][taskSLORatios.AppKey].Clock < taskSLORatios.Clock {
								scaleDownPlan[queuePerf.QueueKey][taskSLORatios.AppKey] = queuePerf
							}
						}
					}
				}
			}
		}

		if len(scaleUpPlan) + len(scaleDownPlan) > 0 {
			l.mutex.Lock()
			if l.MessengerPodUIDsAsPerQueue != nil {

				for i, plan := range []map[string]map[string]*perfstat.QueuePerfMessage{scaleUpPlan, scaleDownPlan} {
					actionType := ScalingActionTypeUp;
					if i == 1 {actionType = ScalingActionTypeDown}

					for queueKey, queueItem := range plan {
						for appKey, appItem := range queueItem {

							actionGroupID := l.generateActionGroupID(appKey, appItem.Clock)

							if _, e := l.actionStatusPerApp[appKey]; !e {
								l.actionStatusPerApp[appKey] = NewActionStatus(appItem.Clock)
							} else if l.actionStatusPerApp[appKey].StartClock < appItem.Clock {
								l.actionStatusPerApp[appKey].Start(appItem.Clock)
							}

							// issue commands
							// command: queueKey, appKey, actionType, ratio, averageRatio

							podUIDs := (*l.MessengerPodUIDsAsPerQueue)(appKey, queueKey)

							action := &ScalingAction{
								AppKey: appKey,
								QueueKey: queueKey,
								PodUIDs: podUIDs,
								ActionType: actionType,
								Ratio: appItem.Ratio,
								AverageRatio: appItem.AverageRatio,
								StartClock: appItem.Clock,
								ActionGroupID: actionGroupID,
							}

							if _, e := l.actionStatusPerApp[appKey].ActionList[actionGroupID]; !e {
								l.actionStatusPerApp[appKey].ActionList[actionGroupID] = []*ScalingAction{}
							}
							l.actionStatusPerApp[appKey].ActionList[actionGroupID] = append(l.actionStatusPerApp[appKey].ActionList[actionGroupID], action)

							go l.SendAction(appKey, queueKey, action)

						}
					}
				}
			}
			l.mutex.Unlock()

		}

	}
}


