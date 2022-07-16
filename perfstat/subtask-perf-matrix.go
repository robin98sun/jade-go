package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	// "uta.edu/aces/jade-go/scheduler"
	// "sync"
	// "time"
)


type SubtaskPerfMatrix struct {
	Length  int
	VectorsOfSubtaskPerf []*SubtaskPerfVector
	VectorKeys map[string]int
}

func NewSubtaskPerfMatrix(length int) *SubtaskPerfMatrix {
	return &SubtaskPerfMatrix{
		Length: length,
	}
}

func (m *SubtaskPerfMatrix) Enqueue(vector *SubtaskPerfVector) *SubtaskPerfVector {
	

	if m.VectorKeys == nil {
		m.VectorKeys = make(map[string]int)
	}

	for key := range vector.SubtaskPerf {
		if _, e := m.VectorKeys[key]; !e {
			m.VectorKeys[key] = 0
		}
		m.VectorKeys[key] += 1
	}

	if m.VectorsOfSubtaskPerf == nil {
		m.VectorsOfSubtaskPerf = []*SubtaskPerfVector{}
	}

	m.VectorsOfSubtaskPerf = append(m.VectorsOfSubtaskPerf, vector)

	if len(m.VectorsOfSubtaskPerf) > m.Length {
		return m.Dequeue()
	}

	return nil
}

func (m *SubtaskPerfMatrix) Dequeue() *SubtaskPerfVector {
	if len(m.VectorsOfSubtaskPerf) == 0 {
		return nil
	}

	itemDequeued := m.VectorsOfSubtaskPerf[0]

	for key, _ := range itemDequeued.SubtaskPerf {
		m.VectorKeys[key] -= 1
	}
	m.VectorsOfSubtaskPerf = m.VectorsOfSubtaskPerf[1:]
	return itemDequeued
}

func (m *SubtaskPerfMatrix) GetValidKeys() []string {
	validKeys := []string{}
	for key, count := range m.VectorKeys {
		if count > 0 {
			validKeys = append(validKeys, key)
		}
	}
	return validKeys
}


