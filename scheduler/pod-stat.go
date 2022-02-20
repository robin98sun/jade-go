package scheduler

import (
	// "math"
	// "sync"
	// "time"
	// "uta.edu/aces/jade-go/kernel"
)

type PodStat struct{
	ServiceTimeHist *Histogram
}

func NewPodStat() *PodStat {
	return &PodStat {
		ServiceTimeHist: &Histogram{},
	}
}
