package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	// "uta.edu/aces/jade-go/scheduler"
	"sync"
	// "time"
)


type SubtaskPerfMatrix struct {
	Length  int
	VectorsOfSubtaskPerf []*SubtaskPerfVector
	TaskKeys map[string]bool
	mutex   *sync.Mutex
}

func NewSubtaskPerfMatrix(length int) *SubtaskPerfMatrix {
	return &SubtaskPerfMatrix{
		Length: length,
		mutex: &sync.Mutex{},
	}
}

func (m *SubtaskPerfMatrix) TaskExist(taskKey string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.TaskKeys != nil {
		if value, e := m.TaskKeys[taskKey]; e && value {
			return true
		}
	}

	return false
}

func (m *SubtaskPerfMatrix) GetVector(taskKey string) *SubtaskPerfVector {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.VectorsOfSubtaskPerf == nil {
		return nil
	}
	for _, vector := range m.VectorsOfSubtaskPerf {
		if vector.GetTaskKey() == taskKey {
			return vector
		}
	}
	return nil
}

func (m *SubtaskPerfMatrix) Enqueue(vector *SubtaskPerfVector) *SubtaskPerfVector {
	
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// taskkeys
	if m.TaskKeys == nil {
		m.TaskKeys = make(map[string]bool)
	}
	m.TaskKeys[vector.GetTaskKey()] = true


	// enqueue the vector
	if m.VectorsOfSubtaskPerf == nil {
		m.VectorsOfSubtaskPerf = []*SubtaskPerfVector{}
	}

	m.VectorsOfSubtaskPerf = append(m.VectorsOfSubtaskPerf, vector)

	// dequeue a vector if exceeding the limit of length
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

	delete(m.TaskKeys, itemDequeued.GetTaskKey())

	m.VectorsOfSubtaskPerf = m.VectorsOfSubtaskPerf[1:]
	return itemDequeued
}

func (m *SubtaskPerfMatrix) GetNodeKeys() map[string]int{
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	nodes := map[string]int{}
	for _, vector := range m.VectorsOfSubtaskPerf {
		if vector.SubtaskPerf == nil {
			continue
		}
		for nodekey, _ := range vector.SubtaskPerf {
			if count, e:= nodes[nodekey]; e{
				nodes[nodekey] = count + 1
			} else {
				nodes[nodekey] = 1
			}
		}
	}

	return nodes
}

func (m *SubtaskPerfMatrix) GetDeadlineViolation(nodekey string) (int, float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	count := 0
	cumulativeTime := float64(0)
	for _, vector := range m.VectorsOfSubtaskPerf {
		if vector.SubtaskPerf == nil {
			continue
		}
		if nodeItem, e := vector.SubtaskPerf[nodekey]; e {
			count += nodeItem.DeadlineViolationCount
			cumulativeTime += nodeItem.DeadlineViolationTime
		} 
	}

	return count, cumulativeTime
}

func (m *SubtaskPerfMatrix) GetDeadlineViolationForAllNodes() (map[string]int, map[string]float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	counts := map[string]int{}
	cumulativeTimes := map[string]float64{}
	for _, vector := range m.VectorsOfSubtaskPerf {
		if vector.SubtaskPerf == nil {
			continue
		}
		for nodekey, nodeItem := range vector.SubtaskPerf {
			if v, e := counts[nodekey]; e{
				counts[nodekey] = v + nodeItem.DeadlineViolationCount
			} else {
				counts[nodekey] = nodeItem.DeadlineViolationCount
			}

			if v, e := cumulativeTimes[nodekey]; e {
				cumulativeTimes[nodekey] = v + nodeItem.DeadlineViolationTime
			} else {
				cumulativeTimes[nodekey] = nodeItem.DeadlineViolationTime
			}
		} 
	}

	return counts, cumulativeTimes
}


