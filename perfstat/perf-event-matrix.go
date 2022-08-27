package perfstat

import (
	"sync"
	// "log"
)

type PerfEventMatrix struct {
	mutex *sync.Mutex
	Vectors []*PerfEventVector
	Length int
	CumulativeVector *PerfEventVector
}

func NewPerfEventMatrix(length int) *PerfEventMatrix {
	return &PerfEventMatrix{
		Length: length,
		mutex: &sync.Mutex{},
		Vectors: []*PerfEventVector{},
		CumulativeVector: NewPerfEventVector(),
	}
}

func (m *PerfEventMatrix) GetLength() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return len(m.Vectors)
}

func (m *PerfEventMatrix) GetInstantCumulativePerfVector() *PerfEventVector {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.CumulativeVector.Copy()
}

func (m *PerfEventMatrix) Enqueue(vector *PerfEventVector) *PerfEventVector {

	if vector == nil {
		return nil
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Vectors = append(m.Vectors, vector)

	m.CumulativeVector.EventClock = vector.EventClock
	for queueKey, perfItem := range vector.QueueSlice {
		if scalar, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scalar.Add(perfItem)
		} else {
			m.CumulativeVector.QueueSlice[queueKey] = perfItem
		}
	}

	for label, taskPerf := range vector.TaskClasses {
		if scalar, e := m.CumulativeVector.TaskClasses[label]; e{
			scalar.Add(taskPerf)
		} else {
			m.CumulativeVector.TaskClasses[label] = taskPerf
		}
	}

	m.CumulativeVector.TaskSLOViolationCount += vector.TaskSLOViolationCount
	m.CumulativeVector.NormalizedTaskSLOViolationCount += vector.NormalizedTaskSLOViolationCount
	m.CumulativeVector.TaskCount += vector.TaskCount
	m.CumulativeVector.Depth++

	var dequeued *PerfEventVector
	if m.Length > 0 && len(m.Vectors) > m.Length {
		dequeued = m.Vectors[0]
		
		// log.Printf("dequeueing vector from PerfEventMatrix")
		// log.Printf("the length before dequeuing is %v", len(m.Vectors))

		m.Vectors = m.Vectors[1:]

		// log.Printf("the length after dequeuing is %v", len(m.Vectors))

		for label, item := range dequeued.QueueSlice {
			if scalar, e := m.CumulativeVector.QueueSlice[label]; e{
				scalar.Minus(item)
			} 
		}
		for label, item := range dequeued.TaskClasses {
			if scalar, e := m.CumulativeVector.TaskClasses[label]; e{
				scalar.Minus(item)
			}
		}

		m.CumulativeVector.TaskSLOViolationCount -= dequeued.TaskSLOViolationCount
		m.CumulativeVector.NormalizedTaskSLOViolationCount -= dequeued.NormalizedTaskSLOViolationCount
		m.CumulativeVector.TaskCount -= dequeued.TaskCount
		m.CumulativeVector.Depth--

	}



	return dequeued
}





