package perfstat

import(
	"sync"
	"math"
	"time"
)

type Clock struct {
	clock uint64
	DaemonIntervalInMilliseconds int
	mutex *sync.Mutex
}

func (c *Clock) increaseClock() {
	if c.clock == math.MaxUint64 {
		c.clock = 0
	} else {
		c.clock++
	}
}

func (c *Clock) daemon() {
	INTERVAL := 100
	if c.DaemonIntervalInMilliseconds > 0 {
		INTERVAL = c.DaemonIntervalInMilliseconds
	}
	for{
		time.Sleep(time.Duration(INTERVAL) * time.Millisecond)

		c.mutex.Lock()

		if c.DaemonIntervalInMilliseconds > 0 && c.DaemonIntervalInMilliseconds != INTERVAL {
			INTERVAL = c.DaemonIntervalInMilliseconds
		}

		c.increaseClock()
		c.mutex.Unlock()
	}
}

func (c *Clock) CurrentClock() uint64 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	x := c.clock

	return x
}


func NewClock() *Clock {
	c := &Clock{
		mutex: &sync.Mutex{},
		DaemonIntervalInMilliseconds: 100,
	}
	go c.daemon()
	return c
}

func CloneClock(c *Clock) *Clock {
	// newClock := &Clock{
	// 	mutex: &sync.Mutex{},
	// 	DaemonIntervalInMilliseconds: c.GetIterationTimeScaleInMilliseconds(),
	// }
	// newClock.SetClock(c.CurrentClock())
	// go newClock.daemon()
	// return newClock

	return c
}

func (c *Clock) SetClock(currentClock uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.clock = currentClock
}

func (c *Clock) GetIterationTimeScaleInMilliseconds() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.DaemonIntervalInMilliseconds
}

func (c *Clock) SetIterationTimeScaleInMilliseconds(timescale int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.DaemonIntervalInMilliseconds = timescale
}
