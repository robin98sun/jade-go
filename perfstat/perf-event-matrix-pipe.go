package perfstat

import (
	"sync"
	"time"
	"math"
	"strconv"
	// "log"
	"uta.edu/aces/jadesdk"
)

type PerfEventMatrixPipe struct {
	Pipe []*PerfEventMatrix
	PipeLength int
	MatrixLength int
	mutex *sync.Mutex
	EventBuffer []*Event
	BasePercentile float64
	QueueClocks map[string]uint64
	EventClock uint64
	Snapshots []*PerfEventVector
	ListenerStarted bool
	MostRecentMatrix *PerfEventMatrix
	DaemonIntervalInMilliseconds int
	// channels to subscribe performance signals
	chanAverageSLOViolationRatio []chan float64
}

func NewPerfEventMatrixPipe(pipeLength int, matrixLength int, basePercentile float64) *PerfEventMatrixPipe {
	pipe := &PerfEventMatrixPipe{
		Pipe: []*PerfEventMatrix{},
		PipeLength: pipeLength,
		MatrixLength: matrixLength,
		mutex: &sync.Mutex{},
		EventBuffer: []*Event{},
		BasePercentile: basePercentile,
		QueueClocks: map[string]uint64{},
		EventClock: 0,
		Snapshots: []*PerfEventVector{},
		chanAverageSLOViolationRatio: []chan float64{},
	}

	go pipe.daemon()

	return pipe
}

func (m *PerfEventMatrixPipe) SetIterationTimeScaleInMilliseconds(timeScale int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.DaemonIntervalInMilliseconds = timeScale
}

func (m *PerfEventMatrixPipe) SubscribeAverageSLOViolationRatio(c chan float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.chanAverageSLOViolationRatio = append(m.chanAverageSLOViolationRatio, c)
}

func (m *PerfEventMatrixPipe) GetQueueClocks() map[string]uint64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.QueueClocks
}


func (m *PerfEventMatrixPipe) StartListener() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.ListenerStarted = true
}

func (m *PerfEventMatrixPipe) StopListener() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.ListenerStarted = false
}

func (m *PerfEventMatrixPipe) GetInstantCumulativePerfVector() *PerfEventVector {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.Pipe) == 0 {
		return nil
	}

	return m.Pipe[0].GetInstantCumulativePerfVector()
}


func (m *PerfEventMatrixPipe) GetEventClock() uint64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.EventClock
}

func (m *PerfEventMatrixPipe) increaseEventClock() uint64 {
	if m.EventClock == math.MaxUint64 {
		m.EventClock = 0
	} else {
		m.EventClock++
	}
	return m.EventClock
}

func (m *PerfEventMatrixPipe) AppendQueueServiceResponseTimeEvent(queueKey string, serviceResponseTime float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	event := &Event{
		EventType: EventTypeQueuePerformance,
		QueuePerf: &QueuePerfItem{
			QueueKey: queueKey,
			ServiceResponseTime: serviceResponseTime,
			Success: 1,
		},
	}

	if m.EventBuffer == nil {
		m.EventBuffer = []*Event{event}
	} else {
		m.EventBuffer = append(m.EventBuffer, event)
	}

}

func (m *PerfEventMatrixPipe) AppendQueueDeadlineViolationEvent(queueKey string, deadlineViolationTime float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	event := &Event{
		EventType: EventTypeQueuePerformance,
		QueuePerf: &QueuePerfItem{
			QueueKey: queueKey,
			DeadlineViolationTime: deadlineViolationTime,
			Hits: 1,
		},
	}
	
	if deadlineViolationTime > 0 {
		event.QueuePerf.DeadlineViolationCount = 1
	}

	if m.EventBuffer == nil {
		m.EventBuffer = []*Event{event}
	} else {
		m.EventBuffer = append(m.EventBuffer, event)
	}

}

func (m *PerfEventMatrixPipe) AppendTaskPerfEvent(tailLatencySLO float64, percentile float64, responseTime float64, relevantQueueEvents []*Event, callback *func(uint64)) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	event := &Event{
		EventType: EventTypeTaskPerformance,
		TaskPerf: &TaskPerfItem{
			TailLatencySLO: tailLatencySLO,
			Percentile: percentile,
			ResponseTime: responseTime,
			Count: 1,
		},
		Callback: callback,
	}

	basePercentile := 0.99
	if m.BasePercentile > 0 && m.BasePercentile < 1 {
		basePercentile = m.BasePercentile
	}
	if responseTime > tailLatencySLO {
		event.TaskPerf.SLOViolationCount = 1
		if percentile > 0 && percentile < 1 {
			event.TaskPerf.NormalizedSLOViolationCount = math.Log(basePercentile) / math.Log(percentile)
		} else {
			event.TaskPerf.NormalizedSLOViolationCount = 1
		}
	}

	m.EventBuffer = append(m.EventBuffer, event)

	if relevantQueueEvents != nil {
		for _, e := range relevantQueueEvents {
			m.EventBuffer = append(m.EventBuffer, e)
		}
	}

}

func (m *PerfEventMatrixPipe) AppendEnvPerfEvent(queueKey string, envMetrics *jadesdk.MetricsEnv) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if envMetrics != nil && envMetrics.CPU != nil && envMetrics.Temperature != nil && envMetrics.System != nil && envMetrics.Voltage != nil {
		event := &Event{
			EventType: EventTypeEnvPerformance,
			EnvPerf: &EnvPerfItem{
				CPUFrequence: envMetrics.CPU.Frequency,
				CPUTemperature: envMetrics.Temperature.Cpu,
				CPUIdle: float64(envMetrics.CPU.Idle),
				SystemContextSwitches: float64(envMetrics.System.ContextSwitches),
				VoltageCore: envMetrics.Voltage.Core,
				Count: 1,
				QueueKey: queueKey,
			},
		}

		m.EventBuffer = append(m.EventBuffer, event)
	}

}


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

		currentClock := m.EventClock

		m.increaseEventClock()

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



