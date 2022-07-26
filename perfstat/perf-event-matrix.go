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
		CumulativeVector: &PerfEventVector{},
	}
}

func (m *PerfEventMatrix) GetInstantCumulativePerfVector() *PerfEventVector {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.CumulativeVector == nil {return nil}
	return m.CumulativeVector.Copy()
}

func (m *PerfEventMatrix) Enqueue(vector *PerfEventVector) *PerfEventVector {

	if vector == nil {return nil}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.Vectors == nil {
		m.Vectors = []*PerfEventVector{vector}
	} else {
		m.Vectors = append(m.Vectors, vector)
	}

	if m.CumulativeVector == nil {
		m.CumulativeVector = vector
	} else {
		m.CumulativeVector.EventClock = vector.EventClock
		for queueKey, perfItem := range vector.QueueSlice {
			if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
				scale.DeadlineViolationCount += perfItem.DeadlineViolationCount
				scale.DeadlineViolationTime += perfItem.DeadlineViolationTime
			} else {
				m.CumulativeVector.QueueSlice[queueKey] = perfItem
			}
		}
		m.CumulativeVector.TaskSLOViolationCount += vector.TaskSLOViolationCount
		m.CumulativeVector.NormalizedTaskSLOViolationCount += vector.NormalizedTaskSLOViolationCount
	}

	if m.Length > 0 && len(m.Vectors) > m.Length {
		dequeued := m.Vectors[0]
		m.Vectors = m.Vectors[1:]

		for queueKey, perfItem := range dequeued.QueueSlice {
			if scale, e := m.CumulativeVector.QueueSlice[queueKey]; e{
				scale.DeadlineViolationCount -= perfItem.DeadlineViolationCount
				scale.DeadlineViolationTime -= perfItem.DeadlineViolationTime
			} 
		}
		m.CumulativeVector.TaskSLOViolationCount -= dequeued.TaskSLOViolationCount
		m.CumulativeVector.NormalizedTaskSLOViolationCount -= dequeued.NormalizedTaskSLOViolationCount

		return dequeued
	}
	return nil
}





