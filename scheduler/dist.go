package scheduler

import (
	"gonum.org/v1/gonum/stat/distuv"
	"strconv"
)

type Dist struct {
	poisson map[string]*distuv.Poisson // mean service time: distPoisson
}

func NewDist() *Dist {
	return &Dist{
		poisson: make(map[string]*distuv.Poisson),
	}
}

func (d *Dist) PoissonRand(mean float64) float64 {
	meanStr := strconv.FormatFloat(mean, 'f', -1, 64)
	if _, e := d.poisson[meanStr]; !e {
		d.poisson[meanStr] = &distuv.Poisson{
			Lambda: mean,
		}
	}
	poissonDist := d.poisson[meanStr]
	return poissonDist.Rand()
}
