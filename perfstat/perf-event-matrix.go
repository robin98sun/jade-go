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

	var dequeued *PerfEventVector = nil
	if m.Length > 0 && len(m.Vectors) > m.Length {
		dequeued = m.Vectors[0]
		
		m.Vectors = m.Vectors[1:]

	}

	m.CumulativeVector.Cumulate(vector, dequeued)

	return dequeued
}






