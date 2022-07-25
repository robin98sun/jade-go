package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	"sync"
	"time"
	"strconv"
	"fmt"
)


// PerfCache
type PerfCache struct {
	TaskCategories map[string]*TaskCategoryItem
	ArrivalRateTracker *ArrivalRateTracker
	mutex *sync.Mutex 
}


func (p *PerfCache) Lock() {
	p.mutex.Lock()
}

func (p *PerfCache) Unlock() {
	p.mutex.Unlock()
}

func NewPerfCache() *PerfCache {
	return &PerfCache{
		TaskCategories: make(map[string]*TaskCategoryItem),
		ArrivalRateTracker: NewArrivalRateTracker(10),
		mutex: &sync.Mutex{},
	}
}

func (p *PerfCache) Clear() {
	p.Lock()
	defer p.Unlock()

	p.TaskCategories = make(map[string]*TaskCategoryItem)
}

func (p *PerfCache) EnqueueArrivalTime(dispatchItem *scheduler.TaskDispatchingItem, arrivalTime time.Time) {

	taskTag := dispatchItem.GetUnifiedTag()

	p.Lock()
	if _, e := p.TaskCategories[taskTag]; !e {
		percentile := dispatchItem.GetPercentile()
		slo := dispatchItem.GetTailLatencySLOInMilliseconds()
		p.TaskCategories[taskTag] = NewTaskCategoryItem(percentile, slo)
	}
	categoryItem := p.TaskCategories[taskTag]
	p.Unlock()

	categoryItem.EnqueueArrivalTime(arrivalTime)
	_, instantOverallArrivalRate := p.ArrivalRateTracker.Enqueue(arrivalTime)

	categoryItem.EnqueueOverallArrivalRate(instantOverallArrivalRate)
}


func (p *PerfCache) EnqueueResponse(dispatchItem *scheduler.TaskDispatchingItem, unloaded_tail_latency float64, adjusted_unloaded_tail_latency float64, finishTimestamp time.Time, subtasks map[string][]*scheduler.TaskCacheSubtaskItem) {

	taskTag := dispatchItem.GetUnifiedTag()

	p.Lock()
	if _, e := p.TaskCategories[taskTag]; !e {
		percentile := dispatchItem.GetPercentile()
		slo := dispatchItem.GetTailLatencySLOInMilliseconds()
		p.TaskCategories[taskTag] = NewTaskCategoryItem(percentile, slo)
	}
	categoryItem := p.TaskCategories[taskTag]
	p.Unlock()

	taskResponseTime := float64(finishTimestamp.Sub(dispatchItem.ArriveTimestamp)/time.Millisecond)

	categoryItem.EnqueueResponse(taskResponseTime, unloaded_tail_latency, adjusted_unloaded_tail_latency, dispatchItem, subtasks)



}

func (p *PerfCache) CollectTraces(traceType string, printf func(string, ...interface{})) [][]string {

	printf("[perf cache] going to collect (%v) traces", traceType)

	p.Lock()
	defer p.Unlock()

	traces := [][]string{}

	headline := []string{
					"task_tag", 
					"matrix_index",
					"vector_index",
					"task_tail_latency_slo_latency(ms)", 
					"task_tail_latency_slo_percentile", 
					"fanout_degree",
					"deadline_violation_count",
					"max_deadline_violation_time(ms)",
					"cumulative_deadline_violation_time(ms)",
					"overall_instant_arrival_rate",
					"task_class_arrival_rate",
					"task_tail_latency",
					"distance_of_tail_to_slo", 
					"unloaded_tail_latency",
					"adjusted_unloaded_tail_latency",
					"memory_consumption",
				}

	if traceType == "full" {
		headline = append(headline, []string{
					"node_key",
					"subtask_deadline_violation_count_on_node", 
					"subtask_deadline_violation_time_on_node(ms)",
				   }...)
	}

	traces = append(traces, headline)

	// prepare nodekeys
	nodeKeySet := make(map[string]bool)

	if traceType == "full" {
		for _, taskCategoryItem := range p.TaskCategories {
			for _, matrix := range taskCategoryItem.MatrixPipeOfSubtaskPerf {
				validKeys := matrix.GetValidKeys()
				for _, key := range validKeys {
					nodeKeySet[key] = true
				}
			}
		}
	}


	// generate traces in a flat table
	for taskTag, taskCategoryItem := range p.TaskCategories {
		minSliceLength := len(taskCategoryItem.MatrixPipeOfSubtaskPerf)
		if len(taskCategoryItem.ArrivalRateTrackers) < minSliceLength {
			minSliceLength = len(taskCategoryItem.ArrivalRateTrackers)
		}
		vector_index := 0
		matrix_index := 0
		for i := minSliceLength-1; i>=0; i-- {
			taskClassArrivalRate := taskCategoryItem.ArrivalRateTrackers[i].GetArrivalRatePerSecond()
			matrix := taskCategoryItem.MatrixPipeOfSubtaskPerf[i]
			matrix_index++
			for j:= 0; j<len(matrix.VectorsOfSubtaskPerf); j++ {
				vector := matrix.VectorsOfSubtaskPerf[j]
				tail := vector.TailLatency
				instantOverallArrivalRate := taskCategoryItem.OverallArrivalRates[i][j]
				line := []string{
					taskTag,
					strconv.Itoa(matrix_index),
					strconv.Itoa(vector_index),
					strconv.FormatFloat(taskCategoryItem.TailLatencySLO, 'f', -1, 64),
					strconv.FormatFloat(taskCategoryItem.PercentilePoint, 'f', -1, 64),
					strconv.Itoa(vector.Fanout),
					strconv.Itoa(vector.DeadlineViolationCount),
					strconv.FormatFloat(vector.MaxDeadlineViolationTime, 'f', -1, 64),
					strconv.FormatFloat(vector.CumulativeDeadlineViolationTime, 'f', -1, 64),
					strconv.FormatFloat(instantOverallArrivalRate, 'f', -1, 64),
					strconv.FormatFloat(taskClassArrivalRate, 'f', -1, 64),
					strconv.FormatFloat(tail, 'f', -1, 64),
					strconv.FormatFloat(taskCategoryItem.TailLatencySLO - tail, 'f', -1, 64),
					strconv.FormatFloat(vector.UnloadedTailLatency, 'f', -1, 64),
					strconv.FormatFloat(vector.AdjustedUnloadedTaillatency, 'f', -1, 64),
					fmt.Sprintf("%v",vector.MemoryOccupation),
				}
				vector_index++

				if traceType == "full" {
					for nodeKey, _ := range nodeKeySet {
						dvc := 0
						dvt := float64(0)
						if nodePerfItem, e := vector.SubtaskPerf[nodeKey]; e {
							dvc = nodePerfItem.DeadlineViolationCount
							dvt = nodePerfItem.DeadlineViolationTime
						}
						nodeline := append(line, []string{
							nodeKey,
							strconv.Itoa(dvc),
							strconv.FormatFloat(dvt, 'f', -1, 64),
						}...)
						traces = append(traces, nodeline)
					}
				} else {
					traces = append(traces, line)
				}

			}
		}

	}
	printf("[perf cache] %v lines of performance traces have been collected", len(traces))
	return traces

}
