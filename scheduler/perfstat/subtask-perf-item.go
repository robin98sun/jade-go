package perfstat

import (
	// "uta.edu/aces/jade-go/histogram"
	// "uta.edu/aces/jade-go/scheduler"
	// "sync"
	// "time"
)

type SubtaskPerfItem struct {
	ResponseTime      		float64 // without queueing, equal with "unloaded service response time"
									// but including communication time
	GivenBudget             float64 // the budget (in milliseconds) has been assigned to the subtask
	DeadlineViolationCount  int
	DeadlineViolationTime 	float64
	CommunicationTime		float64
	QueueingTime            float64
	MostRecentCumulativeDeadlineViolationCountAtBeginning int
	MostRecentCumulativeDeadlineViolationTimeAtBeginning  float64
	MostRecentCumulativeDeadlineViolationCountAtEnd int
	MostRecentCumulativeDeadlineViolationTimeAtEnd float64
}

