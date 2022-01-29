package scheduler

import (
	"strconv"
	"time"
)

// traceType: full / concise; jobKey: the id of which job you want to fetch, "" for all
func (c *TaskCache) CollectTraces(traceType string, jobKey string) [][]string {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	traces := [][]string{}
	headline := []string{}
	
	headline = append(headline, "Job_ID")
	headline = append(headline, "Task_Index")
	headline = append(headline, "Fanout_Degree")
	headline = append(headline, "Module_Name")
	headline = append(headline, "Task_Arrival_Timestamp")
	headline = append(headline, "Subtask_Arrival_Timestamp")

	headline = append(headline, "Task_Total_Time(ms)", "Task_Provision_Time(ms)", "Task_Execution_Time(ms)")
	headline = append(headline, "Task_Parallel_Time(ms)", "Task_Sequential_Time(ms)")
	headline = append(headline, "Subtask_Request_Time(ms)", "Subtask_Parallel_Part_Response_Time(ms)")
	headline = append(headline, "Subtask_Queueing_Time(ms)")
	headline = append(headline, "Queue_Length")
	headline = append(headline, "Subtask_Service_Time(ms)")
	headline = append(headline, "Subtask_Communication_Time(ms)", "Subtask_Upward_Trip_Time(ms)")
	headline = append(headline, "Subtask_Downward_Package_Size", "Subtask_Upward_Package_Size")
	headline = append(headline, "Subtask_Enqueuing_Overhead", "Subtask_Amount_Skipped")
	headline = append(headline, "Subtask_Execution_Time(ms)", "Subtask_PreService_Time(ms)", "Subtask_PostService_Time(ms)")
	if traceType == "full" {
		headline = append(headline, "Task_Budget(ms)", "Task_Priority")
		headline = append(headline, "Retry_Count_Sending", "Retry_Count_Receiving")
		headline = append(headline, "Subtask_Pre_Dispatching_Time(ms)", "Subtask_Report_Processing_Time(ms)")
		headline = append(headline, "Task_Notifying_Aggregator_Time(ms)", "Task_Enqueuing_Worker_Time(ms)", "Task_Post_Execution_Time(ms)")
		headline = append(headline, "Pod_ID", "Task_ID", "Task_Status", "Subtask_ID", "Subtask_Status")

		// MetricsEnv
		headline = append(headline, "Temperature_CPU", "Temperature_Device", "Temperature_Disk")		
		headline = append(headline, "CPU_User", "CPU_Sys", "CPU_Idle", "CPU_Wait", "CPU_Stolen", "CPU_Frequency")
		headline = append(headline, "RAM_Swapped", "RAM_Free", "RAM_Buffer", "RAM_Cache")
		headline = append(headline, "Swap_SwappedIn", "Swap_SwappedOut")
		headline = append(headline, "IO_BlocksReceived", "IO_BlocksSent")
		headline = append(headline, "System_Interrupts", "System_ContextSwitches")
		headline = append(headline, "Processes_Runnable", "Processes_Sleeping")
		headline = append(headline, "Voltage_Core", "Voltage_Sdram")
	}
	traces = append(traces, headline)
	taskIndex := -1
	for _, taskItem := range c.Cache {
		taskIndex++
		for _, dispatchedNode := range taskItem.dispatchedNodes {
			for _, moduleItem := range dispatchedNode.modules {
				for _, subtaskItem := range moduleItem.subtasks {
					if jobKey != "" && jobKey != "all" && jobKey != taskItem.task.Task.JobKey {
						continue
					}
					// keys
					line := []string{}
					
					line = append(line, taskItem.task.Task.JobKey)
					line = append(line, strconv.Itoa(taskIndex))
					line = append(line, strconv.FormatInt(taskItem.Fanout, 10))
					line = append(line, subtaskItem.subtask.ModuleName)

					// timestamps
					timeArr := []time.Time{}
					timeArr = append(timeArr,
						taskItem.task.GetArriveTime(),
						subtaskItem.ArriveTimestamp,
					)
					for _, ts := range timeArr {
						if ts.IsZero() {
							line = append(line, "N/A")
						} else {
							line = append(line, strconv.FormatInt(ts.UnixNano(), 10))
						}
					}

					// task durations milliseconds
					// Task_Total_Time(ms)
					// [6]
					dur := float64(0)
					if !taskItem.task.GetArriveTime().IsZero() && !taskItem.FinishTimestamp.IsZero() {
						dur = float64(float64(taskItem.FinishTimestamp.Sub(taskItem.task.GetArriveTime())) / float64(time.Millisecond))
					}
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))

					// Task_Provision_Time(ms)
					// [7]
					dur = float64(0)
					if !taskItem.task.GetArriveTime().IsZero() && !taskItem.DispatchTimestamp.IsZero() {
						dur = float64(float64(taskItem.DispatchTimestamp.Sub(taskItem.task.GetArriveTime())) / float64(time.Millisecond))
					}
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))

					// Task_Execution_Time(ms)
					// [8]
					dur = float64(0)
					if !taskItem.WorkerReadyTimestamp.IsZero() && !taskItem.LastSubtaskFinishTimestamp.IsZero() {
						dur = float64(float64(taskItem.LastSubtaskFinishTimestamp.Sub(taskItem.WorkerReadyTimestamp)) / float64(time.Millisecond))
					}
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))

					// Task_Parallel_Time
					// [9]
					dur = float64(0)
					if !taskItem.task.GetArriveTime().IsZero() && !taskItem.FinishTimestamp.IsZero() {
						dur = float64(float64(taskItem.WorkerFinishTimestamp.Sub(taskItem.DispatchTimestamp)) / float64(time.Millisecond))
					}
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))

					// Task_Sequential_Time
					// [10]
					dur = float64(0)
					if !taskItem.task.GetArriveTime().IsZero() && !taskItem.FinishTimestamp.IsZero() {
						dur = float64(float64(taskItem.FinishTimestamp.Sub(taskItem.WorkerFinishTimestamp) + taskItem.DispatchTimestamp.Sub(taskItem.task.GetArriveTime())) / float64(time.Millisecond))
					}
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))

					// Subtask_Request_Time(ms)
					// [11]
					dur = float64(float64(subtaskItem.RequestTime) / float64(time.Millisecond))
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
					// Subtask_Parallel_Part_Response_Time(ms)
					// [12]
					dur = float64(float64(subtaskItem.RequestTime - subtaskItem.ForwardingTime - subtaskItem.PreDispatchingTime ) / float64(time.Millisecond))
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
					// Subtask_Queueing_Time(ms)
					// [13]
					dur = float64(float64(subtaskItem.QueueingTime) / float64(time.Millisecond))
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
					// Queue_Length
					// [14]
					line = append(line, strconv.FormatInt(subtaskItem.QueueLength, 10))
					// Subtask_Service_Time(ms)
					// [15]
					dur = float64(float64(subtaskItem.ServiceTime) / float64(time.Millisecond))
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
					// Subtask_Round_Trip_Time(ms)
					// [16]
					dur = float64(float64(subtaskItem.CommunicationTime) / float64(time.Millisecond))
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
					// Subtask_Upward_Trip_Time(ms)
					// [17]
					dur = float64(float64(subtaskItem.ForwardingTime) / float64(time.Millisecond))
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
					// Subtask_Downward_Package_Size
					// [18]
					line = append(line, strconv.Itoa(subtaskItem.SendPackageSize))
					// Subtask_Upward_Package_Size
					// [19]
					line = append(line, strconv.Itoa(subtaskItem.ReceivePackageSize))
					// Subtask_Enqueuing_Overhead
					// [20]
					dur = float64(float64(subtaskItem.EnqueuingOverhead) / float64(time.Millisecond))
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
					// Subtask_Amount_Preempted
					// [21]
					line = append(line, strconv.Itoa(subtaskItem.AmountPreempted))
					// Subtask_Execution_Time 
					// [22]
					dur = float64(float64(subtaskItem.ExecutionTime) / float64(time.Millisecond))
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
					// Subtask_PreService_Time 
					// [23]
					dur = float64(float64(subtaskItem.PreServiceTime) / float64(time.Millisecond))
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
					// Subtask_PostService_Time 
					// [24]
					dur = float64(float64(subtaskItem.PostServiceTime) / float64(time.Millisecond))
					line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))

					var floatValue float64
					var int64Value int64
					var intValue int

					if traceType == "full" {
						// Task_Budget
						line = append(line, strconv.FormatFloat(subtaskItem.Budget, 'f', -1, 64))
						// Task_Priority
						line = append(line, strconv.Itoa(subtaskItem.Priority))
						// Retry_Count_Sending
						line = append(line, strconv.FormatInt(subtaskItem.RetryCountOfSending, 10))
						// Retry_Count_Receiving
						line = append(line, strconv.FormatInt(subtaskItem.RetryCountOfReceiving, 10))
						// Subtask_Pre_Dispatching_Time(ms) 
						dur = float64(float64(subtaskItem.PreDispatchingTime) / float64(time.Millisecond))
						line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
						// Subtask_Report_Processing_Time(ms) 
						dur = float64(float64(subtaskItem.ReportProcessingTime) / float64(time.Millisecond))
						line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
						// Task_Notifying_Aggregator_Time(ms) 
						dur = float64(float64(taskItem.AggregatorReadyTimestamp.Sub(taskItem.DispatchTimestamp)) / float64(time.Millisecond))
						line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
						// Task_Enqueuing_Worker_Time(ms) 
						dur = float64(float64(taskItem.WorkerReadyTimestamp.Sub(taskItem.AggregatorReadyTimestamp)) / float64(time.Millisecond))
						line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
						// Task_Post_Execution_Time(ms) 
						dur = float64(float64(taskItem.FinishTimestamp.Sub(taskItem.LastSubtaskFinishTimestamp)) / float64(time.Millisecond))
						line = append(line, strconv.FormatFloat(dur, 'f', -1, 64))
						// Pod_ID
						line = append(line, subtaskItem.subtask.PodKey)
						// Task_ID
						line = append(line, taskItem.task.Task.GetKey())
						// Task_status
						line = append(line, string(taskItem.status))	
						// Subtask_ID
						line = append(line, subtaskItem.subtask.GetKey())
						// Subtask_status
						line = append(line, string(subtaskItem.status))

						// metrics env
						// Temperature
						// Temperature CPU
						floatValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.Temperature != nil {
							floatValue = subtaskItem.MetricsEnv.Temperature.Cpu
						}
						line = append(line, strconv.FormatFloat(floatValue, 'f', -1, 64))

						// Temperature Device
						floatValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.Temperature != nil {
							floatValue = subtaskItem.MetricsEnv.Temperature.Device
						}
						line = append(line, strconv.FormatFloat(floatValue, 'f', -1, 64))

						// Temperature Disk
						floatValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.Temperature != nil {
							floatValue = subtaskItem.MetricsEnv.Temperature.Disk
						}
						line = append(line, strconv.FormatFloat(floatValue, 'f', -1, 64))

						// CPU User
						intValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.CPU != nil {
							intValue = subtaskItem.MetricsEnv.CPU.User
						}
						line = append(line, strconv.Itoa(intValue))

						// CPU Sys
						intValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.CPU != nil {
							intValue = subtaskItem.MetricsEnv.CPU.Sys
						}
						line = append(line, strconv.Itoa(intValue))

						// CPU Idle
						intValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.CPU != nil {
							intValue = subtaskItem.MetricsEnv.CPU.Idle
						}
						line = append(line, strconv.Itoa(intValue))

						// CPU Wait
						intValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.CPU != nil {
							intValue = subtaskItem.MetricsEnv.CPU.Wait
						}
						line = append(line, strconv.Itoa(intValue))

						// CPU Stolen
						intValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.CPU != nil {
							intValue = subtaskItem.MetricsEnv.CPU.Stolen
						}
						line = append(line, strconv.Itoa(intValue))

						// CPU Frequency
						floatValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.CPU != nil {
							floatValue = subtaskItem.MetricsEnv.CPU.Frequency
						}
						line = append(line, strconv.FormatFloat(floatValue, 'f', -1, 64))

						// RAM Swapped
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.RAM != nil {
							int64Value = subtaskItem.MetricsEnv.RAM.Swapped
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// RAM Free
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.RAM != nil {
							int64Value = subtaskItem.MetricsEnv.RAM.Free
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// RAM Buffer
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.RAM != nil {
							int64Value = subtaskItem.MetricsEnv.RAM.Buffer
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// RAM Cache
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.RAM != nil {
							int64Value = subtaskItem.MetricsEnv.RAM.Cache
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// Swap SwappedIn
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.Swap != nil {
							int64Value = subtaskItem.MetricsEnv.Swap.SwappedIn
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// Swap SwappedOut
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.Swap != nil {
							int64Value = subtaskItem.MetricsEnv.Swap.SwappedOut
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// IO BlocksReceived
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.IO != nil {
							int64Value = subtaskItem.MetricsEnv.IO.BlocksReceived
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// IO BlocksSent
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.IO != nil {
							int64Value = subtaskItem.MetricsEnv.IO.BlocksSent
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// System interrupts
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.System != nil {
							int64Value = subtaskItem.MetricsEnv.System.Interrupts
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// System ContextSwitches
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.System != nil {
							int64Value = subtaskItem.MetricsEnv.System.ContextSwitches
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// Processes Runnable
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.Processes != nil {
							int64Value = subtaskItem.MetricsEnv.Processes.Runnable
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// Processes Sleeping
						int64Value = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.Processes != nil {
							int64Value = subtaskItem.MetricsEnv.Processes.Sleeping
						}
						line = append(line, strconv.FormatInt(int64Value, 10))

						// Voltage Core
						floatValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.Voltage != nil {
							floatValue = subtaskItem.MetricsEnv.Voltage.Core
						}
						line = append(line, strconv.FormatFloat(floatValue, 'f', -1, 64))

						// Voltage Sdram
						floatValue = 0
						if subtaskItem.MetricsEnv != nil && subtaskItem.MetricsEnv.Voltage != nil {
							floatValue = subtaskItem.MetricsEnv.Voltage.Sdram
						}
						line = append(line, strconv.FormatFloat(floatValue, 'f', -1, 64))
					}
					
					// end of trace
					traces = append(traces, line)
				}
			}
		}
	}
	return traces
}