package chef

import (
	"sync"
	"uta.edu/aces/jade-go/kernel"
)

const(
	HistogramSize int = 10000
)

// ref: https://en.wikipedia.org/wiki/Takoyaki
type TakoyakiChef struct {
	mutex 	*sync.Mutex
	Stoves	map[string]*TakoyakiStove // nodes
}

////////////////////////////////////////////////////////////////
// Stove
type TakoyakiStove struct {
	Key      string
	mutex 	 *sync.Mutex
	Pans 	 map[string]*TakoyakiPan // queue per app per module
}

func NewTakoyakiStove(key string) *TakoyakiStove {
	return &TakoyakiStove{
		Key: 	key,
		mutex: 	&sync.Mutex{},
		Pans: 	make(map[string]*TakoyakiPan),
	}
}

////////////////////////////////////////////////////////////////
// Pan
type TakoyakiPan struct {
	Key   			string
	Queue 			*Queue
	Cavities 		map[string]*TakoyakiCavity // allocated resources
	ApplicationID 	string
	ModuleName  	string
	mutex 	 		*sync.Mutex
}

func NewTakoyakiPan(key string, appId string, moduleName string) *TakoyakiPan {
	return &TakoyakiPan{
		Key: key,
		Queue: NewQueue(key, HistogramSize),
		Cavities: make(map[string]*TakoyakiCavity),
		ApplicationID: appId,
		ModuleName: moduleName,
		mutex: &sync.Mutex{},
	}
}

func (p *TakoyakiPan) IsUsable() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if len(p.Cavities) > 0 {
		return true
	}
	return false
}

func (p *TakoyakiPan) LocateOrCreateCavity(resourceId string, capacity *kernel.Capacity) *TakoyakiCavity {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if _, e := p.Cavities[resourceId]; !e {
		p.Cavities[resourceId] = NewTakoyakiCavity(resourceId, capacity)
	}

	return p.Cavities[resourceId]
}

func (p *TakoyakiPan) LocateAndVoidCavity(resourceId string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if _, e := p.Cavities[resourceId]; e {
		delete(p.Cavities, resourceId)
	}
}

func (p *TakoyakiPan) Enqueue(
	targetQueue QueueType, taskKey string, subtaskKey string, payload interface{}, 
	queueingMechanism kernel.TaskQueuingMechanism, maxQueuingTime float64, 
	priority int, estimatedServiceTime float64, // milliseconds
	minCapReq *kernel.Capacity,
	printf func(string, ...interface{}),
) (bool, *QueueItem, int) {
	if !p.IsUsable() {
		return false, nil, 0
	}

	// p.mutex.Lock()
	// defer p.mutex.Unlock()

	queueItemKey := taskKey + ":" + subtaskKey
	return p.Queue.Enqueue(
		targetQueue, queueItemKey, taskKey, subtaskKey, payload,
		queueingMechanism, maxQueuingTime, priority,
		estimatedServiceTime, minCapReq, printf,
	)
}

////////////////////////////////////////////////////////////////
// Cavity
type TakoyakiCavity struct {
	ResourceID     	string
	TakoyakiID		string	// Subtask ID
	Capacity        *kernel.Capacity
	mutex 	 		*sync.Mutex
}

func NewTakoyakiCavity(resId string, cap *kernel.Capacity) *TakoyakiCavity {
	return &TakoyakiCavity{
		ResourceID: resId,
		Capacity: cap,
		mutex: &sync.Mutex{},
	}
}

func (c *TakoyakiCavity) IsInUse() bool {
	if c == nil {return false}
	if c.TakoyakiID == "" {return false}
	return true
}

////////////////////////////////////////////////////////////////
// Chef essential functions
func NewTakoyakiChef() *TakoyakiChef {
	return &TakoyakiChef{
		mutex: &sync.Mutex{},
		Stoves: make(map[string]*TakoyakiStove),
	}
}

func (c *TakoyakiChef) FindPan(nodeKey string, appId string, moduleName string) *TakoyakiPan {

	panKey := appId + ":" + moduleName

	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if stove, e := c.Stoves[nodeKey]; e {
		stove.mutex.Lock()
		defer stove.mutex.Unlock()
		if pan, e := stove.Pans[panKey]; e {
			return pan
		}
	}

	return nil
}


func (c *TakoyakiChef) LocateOrCreatePan(nodeKey string, appId string, moduleName string) *TakoyakiPan {

	panKey := appId + ":" + moduleName

	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	// lazy inialization 
	
	if _, e := c.Stoves[nodeKey]; !e {
		c.Stoves[nodeKey] = NewTakoyakiStove(nodeKey)
	}

	stove := c.Stoves[nodeKey]

	stove.mutex.Lock()
	defer stove.mutex.Unlock()
	if _, e := stove.Pans[panKey]; !e {
		stove.Pans[panKey] = NewTakoyakiPan(panKey, appId, moduleName)
	}

	return stove.Pans[panKey]

}



