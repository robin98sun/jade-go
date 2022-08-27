package perfstat

import (
	"sync"
	"time"
	"math"
	"sort"
	"strconv"
	"log"
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
	}

	go pipe.daemon()

	return pipe
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

	newEvent := &Event{
		EventType: EventTypeQueuePerformance,
		QueuePerf: &QueuePerfItem{
			QueueKey: queueKey,
			ServiceResponseTime: serviceResponseTime,
			Success: 1,
		},
	}

	if m.EventBuffer == nil {
		m.EventBuffer = []*Event{newEvent}
	} else {
		m.EventBuffer = append(m.EventBuffer, newEvent)
	}

}

func (m *PerfEventMatrixPipe) AppendQueueDeadlineViolationEvent(queueKey string, deadlineViolationTime float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	newEvent := &Event{
		EventType: EventTypeQueuePerformance,
		QueuePerf: &QueuePerfItem{
			QueueKey: queueKey,
			DeadlineViolationTime: deadlineViolationTime,
			Hits: 1,
		},
	}
	
	if deadlineViolationTime > 0 {
		newEvent.QueuePerf.DeadlineViolationCount = 1
	}

	if m.EventBuffer == nil {
		m.EventBuffer = []*Event{newEvent}
	} else {
		m.EventBuffer = append(m.EventBuffer, newEvent)
	}

}

func (m *PerfEventMatrixPipe) AppendTaskPerfEvent(tailLatencySLO float64, percentile float64, responseTime float64, relevantQueueEvents []*Event, callback *func(uint64)) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	newEvent := &Event{
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
		newEvent.TaskPerf.SLOViolationCount = 1
		if percentile > 0 && percentile < 1 {
			newEvent.TaskPerf.NormalizedSLOViolationCount = math.Log(basePercentile) / math.Log(percentile)
		} else {
			newEvent.TaskPerf.NormalizedSLOViolationCount = 1
		}
	}

	m.EventBuffer = append(m.EventBuffer, newEvent)

	if relevantQueueEvents != nil {
		for _, e := range relevantQueueEvents {
			m.EventBuffer = append(m.EventBuffer, e)
		}
	}

}


func (m *PerfEventMatrixPipe) daemon() {


	INTERVAL := 100
	for {
		time.Sleep(time.Duration(INTERVAL) * time.Millisecond)

		startTime := time.Now()

		m.mutex.Lock()

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
						vector.QueueSlice[event.QueuePerf.QueueKey] = event.QueuePerf
					}
				} else if event.EventType == EventTypeTaskPerformance {
					vector.TaskSLOViolationCount += event.TaskPerf.SLOViolationCount
					vector.NormalizedTaskSLOViolationCount += event.TaskPerf.NormalizedSLOViolationCount
					vector.TaskCount += event.TaskPerf.Count

					label := strconv.FormatFloat(event.TaskPerf.Percentile, 'f', -1, 64)
					if taskClass, e := vector.TaskClasses[label]; e {
						taskClass.Add(event.TaskPerf)
					} else {
						vector.TaskClasses[label] = event.TaskPerf
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
			dequeued = m.Pipe[i].Enqueue(dequeued)
		}
		if dequeued != nil && (m.PipeLength <= 0 || len(m.Pipe) < m.PipeLength){
			newMatrix := NewPerfEventMatrix(m.MatrixLength)
			newMatrix.Enqueue(dequeued)
			m.Pipe = append(m.Pipe, newMatrix)
			for i:=0;i<len(m.Pipe);i++{
				log.Printf("No.%v matrix in pipe have %v vectors", i, m.Pipe[i].GetLength())
			}
			if m.MostRecentMatrix != nil {
				log.Printf("the most recent matrix have %v vectors", m.MostRecentMatrix.GetLength())
			}
		}
		if len(m.Pipe) == 1 {
			m.MostRecentMatrix = m.Pipe[0]
		}

		if len(m.Pipe) > 0 {
			if m.MostRecentMatrix != m.Pipe[0] {
				log.Printf("the most recent matrix %v equal with No.0 matrix", m.MostRecentMatrix == m.Pipe[0])
			}
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


func (m *PerfEventMatrixPipe) CollectTraces(printf func(string, ...interface{})) [][]string {
	printf("[perf event matrix pipe] going to collect traces")
	m.mutex.Lock()
	defer m.mutex.Unlock()

	traces := [][]string{}

	headline := []string{
					"event_clock",
					"interval",
					"processing_time",
					"recent_task_slo_violation_count",
					"recent_task_slo_violation_normalized_count",
					"recent_average_task_slo_violation_ratio",
					"recent_average_task_slo_exceeding_ratio",
					"recent_average_task_slo_violation_threshold",
					"cumulative_average_task_slo_violation_ratio",
					"cumulative_average_task_slo_exceeding_ratio",
					"depth",
				}


	queueKeys := []string{}
	for queueKey := range m.QueueClocks {
		queueKeys = append(queueKeys, queueKey)
	}

	sort.Strings(queueKeys)

	for _, queueKey := range queueKeys {
		headline = append(headline, queueKey + "::" + "hits")
		headline = append(headline, queueKey + "::" + "success_rate")
		headline = append(headline, queueKey + "::" + "ddl_violation_count")
		headline = append(headline, queueKey + "::" + "ddl_violation_time")
		headline = append(headline, queueKey + "::" + "max_response_count")
		headline = append(headline, queueKey + "::" + "exceeding_slo_count")
		headline = append(headline, queueKey + "::" + "max_and_exceeding_slo_count")
		headline = append(headline, queueKey + "::" + "avg_service_response_time")
		headline = append(headline, queueKey + "::" + "avg_deadline_surplus")
		headline = append(headline, queueKey + "::" + "avg_deadline_surplus_ratio")
	}

	traces = append(traces, headline)

	cumulative_slo_violation_ratio := float64(0)
	cumulative_slo_exceeding_ratio := float64(0)

	for i := 0; i<len(m.Snapshots); i++ {
		snapshot := m.Snapshots[i]

		slo_violation_ratio := snapshot.GetAverageTaskSLOViolationRatio(true)
		slo_exceeding_ratio := snapshot.GetAverageTaskSLOViolationRatio(false)
		cumulative_slo_violation_ratio += slo_violation_ratio
		cumulative_slo_exceeding_ratio += slo_exceeding_ratio
		line := []string{
			strconv.FormatUint(snapshot.EventClock, 10),
			strconv.FormatFloat(snapshot.Interval, 'f', -1, 64),
			strconv.FormatFloat(snapshot.ProcessingTime, 'f', -1, 64),
			strconv.FormatInt(snapshot.TaskSLOViolationCount, 10),
			strconv.FormatFloat(snapshot.NormalizedTaskSLOViolationCount, 'f', -1, 64),
			strconv.FormatFloat(slo_violation_ratio, 'f', -1, 64),
			strconv.FormatFloat(slo_exceeding_ratio, 'f', -1, 64),
			strconv.FormatFloat(snapshot.GetAverageTaskSLOViolationThreshold(), 'f', -1, 64),
			strconv.FormatFloat(cumulative_slo_violation_ratio, 'f', -1, 64),
			strconv.FormatFloat(cumulative_slo_exceeding_ratio, 'f', -1, 64),
			strconv.Itoa(snapshot.Depth),
		}

		for _, queueKey := range queueKeys {
			ddl_violation_count := 0
			ddl_violation_time := float64(0)
			max_response_count := 0
			exceeding_slo_count := 0
			max_and_exceeding_slo_count := 0
			avg_service_response_time := float64(0)
			avg_deadline_surplus := float64(0)
			avg_deadline_surplus_ratio := float64(0)
			hits := 0
			success_rate := float64(0)

			if perfItem, e := snapshot.QueueSlice[queueKey]; e {
				hits = perfItem.Hits
				if hits > 0 {
					success_rate =  float64(perfItem.Success)/float64(hits)
				}
				ddl_violation_count = perfItem.DeadlineViolationCount
				ddl_violation_time = perfItem.DeadlineViolationTime
				max_response_count = perfItem.MaximumResponseCount
				exceeding_slo_count = perfItem.ExceedingTaskSLOCount
				max_and_exceeding_slo_count = perfItem.MaximumAndExceedingTaskSLOCount
				avg_service_response_time = perfItem.ServiceResponseTime / float64(snapshot.Depth)
				avg_deadline_surplus = -perfItem.DeadlineViolationTime / float64(snapshot.Depth)
				avg_deadline_surplus_ratio = 0
				if avg_service_response_time > 0 {
					avg_deadline_surplus_ratio = avg_deadline_surplus / avg_service_response_time
				}
			}

			line = append(line, strconv.Itoa(hits))
			line = append(line, strconv.FormatFloat(success_rate, 'f', -1, 64))
			line = append(line, strconv.Itoa(ddl_violation_count))
			line = append(line, strconv.FormatFloat(ddl_violation_time, 'f', -1, 64))
			line = append(line, strconv.Itoa(max_response_count))
			line = append(line, strconv.Itoa(exceeding_slo_count))
			line = append(line, strconv.Itoa(max_and_exceeding_slo_count))
			line = append(line, strconv.FormatFloat(avg_service_response_time, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(avg_deadline_surplus, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(avg_deadline_surplus_ratio, 'f', -1, 64))
			
		}
		traces = append(traces, line)
	}

	return traces
}

