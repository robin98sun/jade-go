package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/scheduler"
	"sync"
	"time"
	"strconv"
)


// PerfCache
type PerfCache struct {
	TaskCategories map[string]*TaskCategoryItem
	ArrivalRateTracker *ArrivalRateTracker

	mutex *sync.Mutex 

	PerfEventMatrices *PerfEventMatrixPipe
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
		PerfEventMatrices: NewPerfEventMatrixPipe(0, 1000, 0.99),
	}
}

func (p *PerfCache) Clear() {
	p.Lock()
	defer p.Unlock()

	p.TaskCategories = make(map[string]*TaskCategoryItem)
}

func (p *PerfCache) AppendQueueDeadlineViolationEvent(queueKey string, deadlineViolationTime float64) {
	p.PerfEventMatrices.AppendQueueDeadlineViolationEvent(queueKey, deadlineViolationTime)
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

	arrivalClock := p.PerfEventMatrices.GetEventClock()

	_, instantOverallArrivalRate := p.ArrivalRateTracker.Enqueue(arrivalTime)

	categoryItem.ReserveForResponse(arrivalClock, dispatchItem, arrivalTime, instantOverallArrivalRate, p.PerfEventMatrices.GetInstantCumulativePerfVector())
}


func (p *PerfCache) EnqueueResponse(dispatchItem *scheduler.TaskDispatchingItem, unloaded_tail_latency float64, queueing_budget float64, provision_overhead float64, aggregation_overhead float64, adjusted_unloaded_tail_latency float64, finishTimestamp time.Time, subtasks map[string][]*scheduler.TaskCacheSubtaskItem) {

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

	instantOverallArrivalRate := p.ArrivalRateTracker.GetArrivalRatePerSecond()

	perfVector := categoryItem.EnqueueResponse(dispatchItem, taskResponseTime, unloaded_tail_latency, queueing_budget, provision_overhead, aggregation_overhead, adjusted_unloaded_tail_latency, subtasks, instantOverallArrivalRate, p.PerfEventMatrices.GetInstantCumulativePerfVector())

	if perfVector != nil {
		callback := func(responseClock uint64) {
			perfVector.ResponseClock = responseClock
		}
		p.PerfEventMatrices.AppendTaskPerfEvent(
			dispatchItem.GetTailLatencySLOInMilliseconds(),
			dispatchItem.GetPercentile(),
			taskResponseTime,
			perfVector.GetEventsOfStrugglingQueues(dispatchItem.GetTailLatencySLOInMilliseconds(), provision_overhead, aggregation_overhead),
			&callback,
		)
	}

}

func (p *PerfCache) CollectTraces(traceType string, printf func(string, ...interface{})) [][]string {

	printf("[perf cache] going to collect (%v) traces", traceType)

	p.Lock()
	defer p.Unlock()

	traces := [][]string{}

	headline := []string{
					"task_arrival_clock",
					"task_response_clock",
					"task_index_in_cache",
					"task_tag", 
					"matrix_index",
					"task_index_in_matrix",
					"task_tail_latency_slo_latency(ms)", 
					"task_tail_latency",
					"distance_of_tail_to_slo", 
					"task_tail_latency_slo_percentile", 
					"fanout_degree",
					"deadline_violation_count",
					"max_deadline_violation_time(ms)",
					"cumulative_deadline_violation_time(ms)",
					"dispatching_rate",
					"overall_instant_arrival_rate",
					"task_class_arrival_rate",
					"unloaded_tail_latency",
					"queueing_budget",
					"adjusted_unloaded_tail_latency",
					"provision_overhead",
					"aggregation_overhead",
				}

	if traceType == "full" {
		headline = append(headline, []string{
			"queue_key",
			"subtask_deadline_violation_count_on_node", 
			"subtask_deadline_violation_time_on_node(ms)",
			"cumulative_deadline_violation_count_on_node_at_beginning",
			"cumulative_deadline_violation_time_on_node_at_beginning(ms)",
			"cumulative_deadline_violation_count_on_node_at_end",
			"cumulative_deadline_violation_time_on_node_at_end(ms)",
	   }...)
	}

	traces = append(traces, headline)

	// prepare nodekeys
	nodeKeySet := make(map[string]bool)

	if traceType == "full" {
		for _, taskCategoryItem := range p.TaskCategories {
			for _, matrix := range taskCategoryItem.MatrixPipeOfSubtaskPerf {
				validKeys := matrix.GetNodeKeys()
				for key := range validKeys {
					nodeKeySet[key] = true
				}
			}
		}
	}


	// generate traces in a flat table
	vector_index_overall := 0
	for taskTag, taskCategoryItem := range p.TaskCategories {
		matrixCount := len(taskCategoryItem.MatrixPipeOfSubtaskPerf)
		
		matrix_index := 0
		for i := matrixCount-1; i>=0; i-- {
			matrix := taskCategoryItem.MatrixPipeOfSubtaskPerf[i]
			matrix_index++
			vector_index_in_matrix := 0
			for j:= 0; j<len(matrix.VectorsOfSubtaskPerf); j++ {
				vector := matrix.VectorsOfSubtaskPerf[j]
				tail := vector.TailLatency
				line := []string{
					strconv.FormatUint(vector.ArrivalClock, 10),
					strconv.FormatUint(vector.ResponseClock, 10),
					strconv.Itoa(vector_index_overall),
					taskTag,
					strconv.Itoa(matrix_index),
					strconv.Itoa(vector_index_in_matrix),
					strconv.FormatFloat(taskCategoryItem.TailLatencySLO, 'f', -1, 64),
					strconv.FormatFloat(tail, 'f', -1, 64),
					strconv.FormatFloat(taskCategoryItem.TailLatencySLO - tail, 'f', -1, 64),
					strconv.FormatFloat(taskCategoryItem.PercentilePoint, 'f', -1, 64),
					strconv.Itoa(vector.Fanout),
					strconv.Itoa(vector.DeadlineViolationCount),
					strconv.FormatFloat(vector.MaxDeadlineViolationTime, 'f', -1, 64),
					strconv.FormatFloat(vector.CumulativeDeadlineViolationTime, 'f', -1, 64),
					strconv.FormatFloat(vector.DispatchingRate, 'f', -1, 64),
					strconv.FormatFloat(vector.InstantOverallArrivalRateAtBeginning, 'f', -1, 64),
					strconv.FormatFloat(vector.InstantTaskArrivalRateAtBeginning, 'f', -1, 64),
					strconv.FormatFloat(vector.UnloadedTailLatency, 'f', -1, 64),
					strconv.FormatFloat(vector.QueueingBudget, 'f', -1, 64),
					strconv.FormatFloat(vector.AdjustedUnloadedTaillatency, 'f', -1, 64),
					strconv.FormatFloat(vector.ProvisionOverhead, 'f', -1, 64),
					strconv.FormatFloat(vector.AggregationOverhead, 'f', -1, 64),
				}
				vector_index_overall++
				vector_index_in_matrix++

				if traceType == "full" {
					for nodeKey, nodePerfItem := range vector.SubtaskPerf {
						dvc := nodePerfItem.DeadlineViolationCount
						dvt := nodePerfItem.DeadlineViolationTime

						ddlVioCountOnNodeAtBeginning := 0
						ddlVioTimeOnNodeAtBeginning := float64(0)
						if vector.MostRecentCumulativePerfVectorAtBeginning != nil && len(vector.MostRecentCumulativePerfVectorAtBeginning.QueueSlice) > 0 {
							if item, e:= vector.MostRecentCumulativePerfVectorAtBeginning.QueueSlice[nodeKey]; e {
								ddlVioCountOnNodeAtBeginning = item.DeadlineViolationCount
								ddlVioTimeOnNodeAtBeginning = item.DeadlineViolationTime
							}
						}

						ddlVioCountOnNodeAtEnd := 0
						ddlVioTimeOnNodeAtEnd := float64(0)
						if vector.MostRecentCumulativePerfVectorAtEnd != nil && len(vector.MostRecentCumulativePerfVectorAtEnd.QueueSlice) > 0 {
							if item, e := vector.MostRecentCumulativePerfVectorAtEnd.QueueSlice[nodeKey]; e{
								ddlVioCountOnNodeAtEnd = item.DeadlineViolationCount
								ddlVioTimeOnNodeAtEnd = item.DeadlineViolationTime
							}
							
						}
						

						nodeline := append(line, []string{
							nodeKey,
							strconv.Itoa(dvc),
							strconv.FormatFloat(dvt, 'f', -1, 64),
							strconv.Itoa(ddlVioCountOnNodeAtBeginning),
							strconv.FormatFloat(ddlVioTimeOnNodeAtBeginning, 'f', -1, 64),
							strconv.Itoa(ddlVioCountOnNodeAtEnd),
							strconv.FormatFloat(ddlVioTimeOnNodeAtEnd, 'f', -1, 64),
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
