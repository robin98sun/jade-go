package task

// reference:
// https://www.programmersought.com/article/11822267960/
import (
	"strconv"
	"time"

	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

type Dist struct {
	poisson     map[string]*distuv.Poisson     // mean service time: distPoisson
	exponential map[string]*distuv.Exponential // mean service time: distExponential
}

func NewDist() *Dist {
	return &Dist{
		poisson:     make(map[string]*distuv.Poisson),
		exponential: make(map[string]*distuv.Exponential),
	}
}

func (d *Dist) PoissonRand(mean float64) float64 {
	meanStr := strconv.FormatFloat(mean, 'f', -1, 64)
	if _, e := d.poisson[meanStr]; !e {
		d.poisson[meanStr] = &distuv.Poisson{
			Lambda: mean,
			Src:    rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
		}
	}
	poissonDist := d.poisson[meanStr]
	return poissonDist.Rand()
}

func (d *Dist) ExponentialRand(mean float64) float64 {
	meanStr := strconv.FormatFloat(mean, 'f', -1, 64)
	if _, e := d.exponential[meanStr]; !e {
		d.exponential[meanStr] = &distuv.Exponential{
			Rate: 1 / mean,
			Src:  rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
		}
	}
	exponentialDist := d.exponential[meanStr]
	return exponentialDist.Rand()
}
