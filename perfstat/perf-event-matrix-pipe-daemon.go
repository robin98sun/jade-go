package perfstat

import (
	"time"
	"strconv"
)


func (m *PerfEventMatrixPipe) daemon() {


	INTERVAL := 100
	if m.DaemonIntervalInMilliseconds > 0 {
		INTERVAL = m.DaemonIntervalInMilliseconds
	}
	for {
		time.Sleep(time.Duration(INTERVAL) * time.Millisecond)

		startTime := time.Now()

		m.mutex.Lock()

		if m.DaemonIntervalInMilliseconds > 0 && m.DaemonIntervalInMilliseconds != INTERVAL {
			INTERVAL = m.DaemonIntervalInMilliseconds
		}

		if !m.ListenerStarted {
			m.mutex.Unlock()
			continue
		}

		currentClock := m.clock.CurrentClock()

		vector := NewPerfEventVector()
		vector.EventClock = currentClock
		vector.Interval = float64(INTERVAL)

		if len(m.EventBuffer) > 0 {
			for _, event := range m.EventBuffer {
				if event.EventType == EventTypeQueuePerformance {
					if m.QueueClocks == nil {
						m.QueueClocks = map[string]uint64{}
					}
					m.QueueClocks[event.QueuePerf.QueueKey] = currentClock

					if scale, e := vector.QueueSlice[event.QueuePerf.QueueKey]; e {
						scale.Add(event.QueuePerf)
					} else {
						vector.QueueSlice[event.QueuePerf.QueueKey] = event.QueuePerf.Copy()
					}
				} else if event.EventType == EventTypeTaskPerformance {
					vector.TaskSLOViolationCount += event.TaskPerf.SLOViolationCount
					vector.NormalizedTaskSLOViolationCount += event.TaskPerf.NormalizedSLOViolationCount
					vector.TaskCount += event.TaskPerf.Count

					label := strconv.FormatFloat(event.TaskPerf.Percentile, 'f', -1, 64)
					if taskClass, e := vector.TaskClasses[label]; e {
						taskClass.Add(event.TaskPerf)
					} else {
						vector.TaskClasses[label] = event.TaskPerf.Copy()
					}
				} else if event.EventType == EventTypeEnvPerformance {
					if envPerf, e := vector.InstantEnvPerf[event.EnvPerf.QueueKey]; e {
						envPerf.Add(event.EnvPerf)
					} else {
						vector.InstantEnvPerf[event.EnvPerf.QueueKey] = event.EnvPerf.Copy()
					}
				}

				if event.Callback != nil {
					go (*event.Callback)(currentClock)
				}
				
			}
			m.EventBuffer = []*Event{}
		}

		dequeued := vector
		for i:=0; i<len(m.Pipe); i++ {
			if dequeued == nil {
				break
			}
			dequeued = m.Pipe[i].Enqueue(dequeued)
		}
		if dequeued != nil && (m.PipeLength <= 0 || len(m.Pipe) < m.PipeLength){
			newMatrix := NewPerfEventMatrix(m.MatrixLength)
			newMatrix.Enqueue(dequeued)
			m.Pipe = append(m.Pipe, newMatrix)
		}
		if len(m.Pipe) == 1 {
			m.MostRecentMatrix = m.Pipe[0]
		}

		if m.MostRecentMatrix != nil {
			snapshot := m.MostRecentMatrix.GetInstantCumulativePerfVector()
			m.Snapshots = append(m.Snapshots, snapshot)
			if m.MatrixLength > 0 && m.PipeLength > 0 {
				if len(m.Snapshots) > m.MatrixLength * m.PipeLength {
					m.Snapshots = m.Snapshots[1:]
				}
			}

			endTime := time.Now()
			vector.ProcessingTime = float64(endTime.Sub(startTime))/float64(time.Millisecond)
			snapshot.Interval = vector.Interval
			snapshot.ProcessingTime = vector.ProcessingTime
		}

		m.mutex.Unlock()

	}
}


