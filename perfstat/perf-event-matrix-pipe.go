package perfstat

import (
	"sync"
	"math"
	"uta.edu/aces/jadesdk"
)

type PerfEventMatrixPipe struct {
	clock *Clock
	Pipe []*PerfEventMatrix
	PipeLength int
	MatrixLength int
	mutex *sync.Mutex
	EventBuffer []*Event
	BasePercentile float64
	QueueClocks map[string]uint64
	Snapshots []*PerfEventVector
	ListenerStarted bool
	MostRecentMatrix *PerfEventMatrix
	DaemonIntervalInMilliseconds int
	// channels to subscribe performance signals
	chanAverageSLOViolationRatio []chan float64
}

func NewPerfEventMatrixPipe(clock *Clock, pipeLength int, matrixLength int, basePercentile float64) *PerfEventMatrixPipe {
	pipe := &PerfEventMatrixPipe{
		clock: clock,
		Pipe: []*PerfEventMatrix{},
		PipeLength: pipeLength,
		MatrixLength: matrixLength,
		mutex: &sync.Mutex{},
		EventBuffer: []*Event{},
		BasePercentile: basePercentile,
		QueueClocks: map[string]uint64{},
		Snapshots: []*PerfEventVector{},
		chanAverageSLOViolationRatio: []chan float64{},
		DaemonIntervalInMilliseconds: 100,
	}

	go pipe.daemon()

	return pipe
}

func (m *PerfEventMatrixPipe) SetIterationTimeScaleInMilliseconds(timeScale int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.DaemonIntervalInMilliseconds = timeScale
}

func (m *PerfEventMatrixPipe) SetHistoryTimeWindowSize(winodwSize int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	newPipeLength := winodwSize / m.MatrixLength
	if newPipeLength < m.PipeLength {
		m.Pipe = m.Pipe[0:newPipeLength]
	}
	m.PipeLength = newPipeLength

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




