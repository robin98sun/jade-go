package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	// "uta.edu/aces/jade-go/scheduler"
	"sync"
	// "time"
)


type TaskPerfMatrix struct {
	Length  int
	VectorsOfSubtaskPerf []*TaskPerfVector
	TaskKeys map[string]bool
	mutex   *sync.Mutex
}

func NewTaskPerfMatrix(length int) *TaskPerfMatrix {
	return &TaskPerfMatrix{
		Length: length,
		mutex: &sync.Mutex{},
	}
}

func (m *TaskPerfMatrix) GetVectorCount() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return len(m.VectorsOfSubtaskPerf)
}

func (m *TaskPerfMatrix) TaskExist(taskKey string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.TaskKeys != nil {
		if value, e := m.TaskKeys[taskKey]; e && value {
			return true
		}
	}

	return false
}

func (m *TaskPerfMatrix) GetVector(taskKey string) (int, *TaskPerfVector) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.VectorsOfSubtaskPerf == nil {
		return -1, nil
	}
	for i, vector := range m.VectorsOfSubtaskPerf {
		if vector.GetTaskKey() == taskKey {
			return i, vector
		}
	}
	return -1, nil
}

func (m *TaskPerfMatrix) RemoveVector(taskKey string) int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.TaskKeys != nil {
		if value, e := m.TaskKeys[taskKey]; e && value {
			idx := -1
			for i, vector := range m.VectorsOfSubtaskPerf {
				if vector.GetTaskKey() == taskKey {
					idx = i
					break
				}
			}
			if idx >= 0 {

				tmpList := []*TaskPerfVector{}
				for i:= 0; i<idx; i++ {
					tmpList = append(tmpList, m.VectorsOfSubtaskPerf[i])
				}
				for i:= idx+1; i<len(m.VectorsOfSubtaskPerf); i++ {
					tmpList = append(tmpList, m.VectorsOfSubtaskPerf[i])
				}
				m.VectorsOfSubtaskPerf = tmpList
				delete(m.TaskKeys, taskKey)
				return idx
			}
		}
	}
	return -1
}

func (m *TaskPerfMatrix) Enqueue(vector *TaskPerfVector) *TaskPerfVector {
	
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// taskkeys
	if m.TaskKeys == nil {
		m.TaskKeys = make(map[string]bool)
	}
	m.TaskKeys[vector.GetTaskKey()] = true


	// enqueue the vector
	if m.VectorsOfSubtaskPerf == nil {
		m.VectorsOfSubtaskPerf = []*TaskPerfVector{}
	}

	m.VectorsOfSubtaskPerf = append(m.VectorsOfSubtaskPerf, vector)

	// dequeue a vector if exceeding the limit of length
	if len(m.VectorsOfSubtaskPerf) > m.Length {
		return m.Dequeue()
	}

	return nil
}

func (m *TaskPerfMatrix) Dequeue() *TaskPerfVector {
	if len(m.VectorsOfSubtaskPerf) == 0 {
		return nil
	}
	itemDequeued := m.VectorsOfSubtaskPerf[0]

	delete(m.TaskKeys, itemDequeued.GetTaskKey())

	m.VectorsOfSubtaskPerf = m.VectorsOfSubtaskPerf[1:]
	return itemDequeued
}

func (m *TaskPerfMatrix) GetNodeKeys() map[string]int{
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

func (m *TaskPerfMatrix) GetDeadlineViolation(nodekey string) (int, float64) {
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

func (m *TaskPerfMatrix) GetDeadlineViolationForAllNodes() (map[string]int, map[string]float64) {
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


