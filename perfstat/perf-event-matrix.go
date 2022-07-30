package perfstat

import (
	"sync"
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

func (m *PerfEventMatrix) GetInstantCumulativePerfVector() *PerfEventVector {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.CumulativeVector.Copy()
}

func (m *PerfEventMatrix) Enqueue(vector *PerfEventVector) *PerfEventVector {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Vectors = append(m.Vectors, vector)

	m.CumulativeVector.EventClock = vector.EventClock
	for queueKey, perfItem := range vector.QueueSlice {
		if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
			scale.Add(perfItem)
		} else {
			m.CumulativeVector.QueueSlice[queueKey] = perfItem
		}
	}
	m.CumulativeVector.TaskSLOViolationCount += vector.TaskSLOViolationCount
	m.CumulativeVector.NormalizedTaskSLOViolationCount += vector.NormalizedTaskSLOViolationCount
	m.CumulativeVector.Depth++

	if m.Length > 0 && len(m.Vectors) > m.Length {
		dequeued := m.Vectors[0]
		m.Vectors = m.Vectors[1:]

		for queueKey, perfItem := range dequeued.QueueSlice {
			if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
				scale.Minus(perfItem)
			} 
		}
		m.CumulativeVector.TaskSLOViolationCount -= dequeued.TaskSLOViolationCount
		m.CumulativeVector.NormalizedTaskSLOViolationCount -= dequeued.NormalizedTaskSLOViolationCount
		m.CumulativeVector.Depth--

		return dequeued
	}
	return nil
}





