package perfstat

import(
	"sort"
	"strconv"
)

func (p *TaskCategoriesCache) CollectTraces(traceType string, queueSet map[string]uint64, printf func(string, ...interface{})) [][]string {

	printf("[perf cache] going to collect (%v) traces", traceType)

	p.mutex.Lock()
	defer p.mutex.Unlock()

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

    sortedQueueKeys := []string{}
    for queueKey, _ := range queueSet {
    	sortedQueueKeys = append(sortedQueueKeys, queueKey)
    }
    sort.Strings(sortedQueueKeys)

	if traceType == "full" {
		for _, queueKey := range sortedQueueKeys {
			headline = append(headline, []string{
				queueKey + "::subtask_deadline_violation_count_on_node", 
				queueKey + "::subtask_deadline_violation_time_on_node(ms)",
				queueKey + "::cumulative_deadline_violation_count_on_node_at_beginning",
				queueKey + "::cumulative_deadline_violation_time_on_node_at_beginning(ms)",
				queueKey + "::cumulative_deadline_violation_count_on_node_at_end",
				queueKey + "::cumulative_deadline_violation_time_on_node_at_end(ms)",
				queueKey + "::budget(ms)",
				queueKey + "::service_response_time(ms)",
				queueKey + "::communication_time(ms)",
				queueKey + "::queueing_time(ms)",
		   }...)
		}
		
	}

	traces = append(traces, headline)


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
					strconv.FormatUint(vector.ArrivalEventClock, 10),
					strconv.FormatUint(vector.ResponseEventClock, 10),
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
					for _, queueKey := range sortedQueueKeys {
						dvc := 0
						dvt := float64(0)
						ddlVioCountOnNodeAtBeginning := 0
						ddlVioTimeOnNodeAtBeginning := float64(0)
						ddlVioCountOnNodeAtEnd := 0
						ddlVioTimeOnNodeAtEnd := float64(0)
						serviceResponseTime := float64(0)
						queueingTime := float64(0)
						communicationTime := float64(0)
						budget := float64(0)

						if nodePerfItem, e := vector.SubtaskPerf[queueKey]; e {
							dvc = nodePerfItem.DeadlineViolationCount
							dvt = nodePerfItem.DeadlineViolationTime

							serviceResponseTime = nodePerfItem.ResponseTime
							queueingTime =nodePerfItem.QueueingTime
							communicationTime = nodePerfItem.CommunicationTime
							budget = nodePerfItem.GivenBudget

							if vector.MostRecentCumulativePerfVectorAtBeginning != nil && len(vector.MostRecentCumulativePerfVectorAtBeginning.QueueSlice) > 0 {
								if item, e:= vector.MostRecentCumulativePerfVectorAtBeginning.QueueSlice[queueKey]; e {
									ddlVioCountOnNodeAtBeginning = item.DeadlineViolationCount
									ddlVioTimeOnNodeAtBeginning = item.DeadlineViolationTime
								}
							}
							if vector.MostRecentCumulativePerfVectorAtEnd != nil && len(vector.MostRecentCumulativePerfVectorAtEnd.QueueSlice) > 0 {
								if item, e := vector.MostRecentCumulativePerfVectorAtEnd.QueueSlice[queueKey]; e{
									ddlVioCountOnNodeAtEnd = item.DeadlineViolationCount
									ddlVioTimeOnNodeAtEnd = item.DeadlineViolationTime
								}
								
							}
						}

						line = append(line, 
							strconv.Itoa(dvc),
							strconv.FormatFloat(dvt, 'f', -1, 64),
							strconv.Itoa(ddlVioCountOnNodeAtBeginning),
							strconv.FormatFloat(ddlVioTimeOnNodeAtBeginning, 'f', -1, 64),
							strconv.Itoa(ddlVioCountOnNodeAtEnd),
							strconv.FormatFloat(ddlVioTimeOnNodeAtEnd, 'f', -1, 64),
							strconv.FormatFloat(budget, 'f', -1, 64),
							strconv.FormatFloat(serviceResponseTime, 'f', -1, 64),
							strconv.FormatFloat(communicationTime, 'f', -1, 64),
							strconv.FormatFloat(queueingTime, 'f', -1, 64),
						)
					}
				}
				traces = append(traces, line)
			}
		}

	}
	printf("[perf cache] %v lines of performance traces have been collected", len(traces))
	return traces

}


