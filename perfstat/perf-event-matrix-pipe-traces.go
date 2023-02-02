package perfstat



import (
	"sort"
	"strconv"
)


func (m *PerfEventMatrixPipe) CollectTraces(printf func(string, ...interface{})) [][]string {
	printf("[perf event matrix pipe] going to collect traces")
	m.mutex.Lock()
	defer m.mutex.Unlock()

	traces := [][]string{}

	headline := []string{
					"event_clock",
					"interval",
					"processing_time",
					"recent_task_slo_violation_count",
					"recent_task_slo_violation_normalized_count",
					"recent_average_task_slo_violation_ratio",
					"recent_average_task_slo_exceeding_ratio",
					"recent_average_task_slo_violation_threshold",
					"cumulative_average_task_slo_violation_ratio",
					"cumulative_average_task_slo_exceeding_ratio",
					"depth",
				}


	queueKeys := []string{}
	for queueKey := range m.QueueClocks {
		queueKeys = append(queueKeys, queueKey)
	}

	sort.Strings(queueKeys)

	for _, queueKey := range queueKeys {
		headline = append(headline, queueKey + "::" + "hits")
		headline = append(headline, queueKey + "::" + "success_rate")
		headline = append(headline, queueKey + "::" + "ddl_violation_count")
		headline = append(headline, queueKey + "::" + "ddl_violation_time")
		headline = append(headline, queueKey + "::" + "max_response_count")
		headline = append(headline, queueKey + "::" + "exceeding_slo_count")
		headline = append(headline, queueKey + "::" + "max_and_exceeding_slo_count")
		headline = append(headline, queueKey + "::" + "avg_service_response_time")
		headline = append(headline, queueKey + "::" + "avg_deadline_surplus")
		headline = append(headline, queueKey + "::" + "avg_deadline_surplus_ratio")
		headline = append(headline, queueKey + "::" + "cpu_frequency")
		headline = append(headline, queueKey + "::" + "cpu_temperature")
		headline = append(headline, queueKey + "::" + "cpu_idle")
		headline = append(headline, queueKey + "::" + "system_context_switches")
		headline = append(headline, queueKey + "::" + "voltage_core")
		headline = append(headline, queueKey + "::" + "avg_cpu_frequency")
		headline = append(headline, queueKey + "::" + "avg_cpu_temperature")
		headline = append(headline, queueKey + "::" + "avg_cpu_idle")
		headline = append(headline, queueKey + "::" + "avg_system_context_switches")
		headline = append(headline, queueKey + "::" + "avg_voltage_core")
	}

	traces = append(traces, headline)

	cumulative_slo_violation_ratio := float64(0)
	cumulative_slo_exceeding_ratio := float64(0)

	for i := 0; i<len(m.Snapshots); i++ {
		snapshot := m.Snapshots[i]

		slo_violation_ratio := snapshot.GetAverageTaskSLOViolationRatio(true)
		slo_exceeding_ratio := snapshot.GetAverageTaskSLOViolationRatio(false)
		cumulative_slo_violation_ratio += slo_violation_ratio
		cumulative_slo_exceeding_ratio += slo_exceeding_ratio
		line := []string{
			strconv.FormatUint(snapshot.EventClock, 10),
			strconv.FormatFloat(snapshot.Interval, 'f', -1, 64),
			strconv.FormatFloat(snapshot.ProcessingTime, 'f', -1, 64),
			strconv.FormatInt(snapshot.TaskSLOViolationCount, 10),
			strconv.FormatFloat(snapshot.NormalizedTaskSLOViolationCount, 'f', -1, 64),
			strconv.FormatFloat(slo_violation_ratio, 'f', -1, 64),
			strconv.FormatFloat(slo_exceeding_ratio, 'f', -1, 64),
			strconv.FormatFloat(snapshot.GetAverageTaskSLOViolationThreshold(), 'f', -1, 64),
			strconv.FormatFloat(cumulative_slo_violation_ratio, 'f', -1, 64),
			strconv.FormatFloat(cumulative_slo_exceeding_ratio, 'f', -1, 64),
			strconv.Itoa(snapshot.Depth),
		}

		for _, queueKey := range queueKeys {
			ddl_violation_count := 0
			ddl_violation_time := float64(0)
			max_response_count := 0
			exceeding_slo_count := 0
			max_and_exceeding_slo_count := 0
			avg_service_response_time := float64(0)
			avg_deadline_surplus := float64(0)
			avg_deadline_surplus_ratio := float64(0)
			hits := 0
			success_rate := float64(0)

			if perfItem, e := snapshot.QueueSlice[queueKey]; e {
				hits = perfItem.Hits
				if hits > 0 {
					success_rate =  float64(perfItem.Success)/float64(hits)
				}
				ddl_violation_count = perfItem.DeadlineViolationCount
				ddl_violation_time = perfItem.DeadlineViolationTime
				max_response_count = perfItem.MaximumResponseCount
				exceeding_slo_count = perfItem.ExceedingTaskSLOCount
				max_and_exceeding_slo_count = perfItem.MaximumAndExceedingTaskSLOCount
				avg_service_response_time = perfItem.ServiceResponseTime / float64(hits)
				avg_deadline_surplus = -perfItem.DeadlineViolationTime / float64(hits)
				avg_deadline_surplus_ratio = 0
				if avg_service_response_time > 0 {
					avg_deadline_surplus_ratio = avg_deadline_surplus / avg_service_response_time
				}
			}

			line = append(line, strconv.Itoa(hits))
			line = append(line, strconv.FormatFloat(success_rate, 'f', -1, 64))
			line = append(line, strconv.Itoa(ddl_violation_count))
			line = append(line, strconv.FormatFloat(ddl_violation_time, 'f', -1, 64))
			line = append(line, strconv.Itoa(max_response_count))
			line = append(line, strconv.Itoa(exceeding_slo_count))
			line = append(line, strconv.Itoa(max_and_exceeding_slo_count))
			line = append(line, strconv.FormatFloat(avg_service_response_time, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(avg_deadline_surplus, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(avg_deadline_surplus_ratio, 'f', -1, 64))


			cpu_frequency := float64(0)
			cpu_temperature := float64(0)
			cpu_idle := float64(0)
			system_context_switches := float64(0)
			Voltage_core := float64(0)

			if envPerf, e := snapshot.InstantEnvPerf[queueKey]; e {
				cpu_frequency = envPerf.CPUFrequence
				cpu_temperature = envPerf.CPUTemperature
				cpu_idle = envPerf.CPUIdle
				system_context_switches = envPerf.SystemContextSwitches
				Voltage_core = envPerf.VoltageCore
			}
			line = append(line, strconv.FormatFloat(cpu_frequency, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(cpu_temperature, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(cpu_idle, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(system_context_switches, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(Voltage_core, 'f', -1, 64))


			avg_cpu_frequency := float64(0)
			avg_cpu_temperature := float64(0)
			avg_cpu_idle := float64(0)
			avg_system_context_switches := float64(0)
			avg_Voltage_core := float64(0)

			if envPerf, e := snapshot.AvgEnvPerf[queueKey]; e {
				avg_cpu_frequency = envPerf.CPUFrequence
				avg_cpu_temperature = envPerf.CPUTemperature
				avg_cpu_idle = envPerf.CPUIdle
				avg_system_context_switches = envPerf.SystemContextSwitches
				avg_Voltage_core = envPerf.VoltageCore
			}
			line = append(line, strconv.FormatFloat(avg_cpu_frequency, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(avg_cpu_temperature, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(avg_cpu_idle, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(avg_system_context_switches, 'f', -1, 64))
			line = append(line, strconv.FormatFloat(avg_Voltage_core, 'f', -1, 64))
			
		}
		traces = append(traces, line)
	}

	return traces
}