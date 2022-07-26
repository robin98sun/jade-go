package scheduler

import (
	"sync"
	"time"
	"uta.edu/aces/jade-go/histogram"
	"uta.edu/aces/jade-go/kernel"
)

// the shape of task cache:
// for each task, there should have a cache for each sub-node it has been dispatched
// [task-key]: {
// 		task: task-instance,
//      status: task-status,
// 		[node-key]: {
// 			node: node-instance,
// 			status: task-status,
// 			[moduleName]: {
//            status: task-status,
// 			  [sub-task-key]: {
// 				    subtask: sub-task-instance,
// . 				status: sub-task-status,
//  				updates: sub-task-result,
// 	  		}
// .    }
// 		}
// }

// the responsibility of task cache is:
// to retain a task after it has been dispatched to sub-nodes, and wait for response from them
// after all subnodes have accepted the task, and return the corresponding pod for that subtask
// the task cache then could dispatch the task to an aggregator, which is a task coordinator
// after dispatching pods in sub-nodes to the aggregator(task coordinator)
// the sub-tasks could be enqueued into the pod-queue for that sub-task

// when a sub-task is done, it will update it's result into the task cache

type TaskCache struct {
	Cache map[string]*TaskCacheTaskItem
	mutex *sync.Mutex
	// Stat  map[string]map[string]map[string]*jadesdk.Stat // app -> module -> fanout degree -> stat
}

func NewTaskCache() *TaskCache {
	inst := &TaskCache{
		Cache: make(map[string]*TaskCacheTaskItem),
		mutex: &sync.Mutex{},
		// Stat:  make(map[string]map[string]map[string]*jadesdk.Stat),
	}
	return inst
}

func (c *TaskCache) Describe() map[string]interface{} {
	if c == nil {
		return nil
	}
	cache := make(map[string]interface{})
	for key, item := range c.Cache {
		cache[key] = item.describe()
	}
	return cache
}

func (c *TaskCache) GetTask(taskID string, lock bool) *TaskDispatchingItem {
	if taskID == "" {
		return nil
	}

	if lock {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}

	if item, exists := c.Cache[taskID]; exists {
		return item.task
	}
	return nil
}

func (c *TaskCache) Clear(seconds int) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.Cache == nil {
		return 0
	}

	tasksToClear := []string{}
	for k, taskItem := range c.Cache {
		if int64(time.Now().Sub(taskItem.task.ArriveTimestamp) / time.Second) > int64(seconds) {
			tasksToClear = append(tasksToClear, k)
		}
	}

	for _, k := range tasksToClear {
		delete(c.Cache, k)
	}

	if seconds <= 0 {	
		c.Cache = make(map[string]*TaskCacheTaskItem)
	}

	return len(tasksToClear)

}

func (c *TaskCache) SetBudgetNegotiationCache(taskId string, cache *BudgetNegotiationResponseCache) {
	if taskId == "" {return}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if item, exists := c.Cache[taskId]; exists {
		item.BudgetNegotiationCache = cache
	}
}

func (c *TaskCache) GetBudgetNegotiationCache(taskId string) *BudgetNegotiationResponseCache {
	if taskId == "" {return nil}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if item, exists := c.Cache[taskId]; exists {
		return item.BudgetNegotiationCache 
	}
	return nil

}

type TaskCacheTaskItem struct {
	task                *TaskDispatchingItem
	dispatchedNodes     map[string]*TaskCacheNodeItem // node-key : nodeItem
	status              TaskStatus
	LastUpdateTimestamp time.Time `json:"lastUpdateTimestamp,omitempty"`
	DispatchTimestamp   time.Time `json:"dispatchTimestamp,omitempty"`
	AggregatorReadyTimestamp time.Time `json:"aggregatorReadyTimestamp,omitempty"`
	WorkerReadyTimestamp time.Time `json:"workerReadyTimestamp,omitempty"`
	LastSubtaskFinishTimestamp time.Time `json:"lastSubtaskFinishTimestamp,omitempty"`
	WorkerFinishTimestamp time.Time `json:"workerFinishiTimestamp,omitempty"`
	FinishTimestamp     time.Time `json:"finishTimestamp,omitempty"`
	Fanout              int64     `json:"fanout,omitempty"`
	AcceptTimestamp    time.Time     `json:"acceptTimestamp,omitempty"`
	UnloadedTailLatency float64 `jason:"unloadedTail,omitempty"`
	Budget 				float64 `jason:"budget,omitempty"`
	BudgetNegotiationCache *BudgetNegotiationResponseCache `json:"budgetNegotiationCache,omitempty"`
}

func NewTaskCacheTaskItem(taskItem *TaskDispatchingItem) *TaskCacheTaskItem {
	item := &TaskCacheTaskItem{
		task:            taskItem,
		dispatchedNodes: make(map[string]*TaskCacheNodeItem),
		status:          TaskStatusPending,
	}
	return item
}

func (i *TaskCacheTaskItem) describe() map[string]interface{} {
	if i == nil {
		return nil
	}
	dispatchedNodes := make(map[string]interface{})
	for key, node := range i.dispatchedNodes {
		dispatchedNodes[key] = node.describe()
	}
	desc := map[string]interface{}{
		"task":            i.task.Task,
		"dispatchedNodes": dispatchedNodes,
		"status":          i.status,
	}
	return desc
}

// CheckTaskStatus check whether a task is totally accepted or rejected by all worker nodes, or totally done,
// return accepted/rejected/waiting/invalid/done
func (t *TaskCacheTaskItem) CheckStatus() TaskStatus {
	if t == nil {
		return TaskStatusInvalid
	}

	items := []*ObjWithTaskStatus{}
	for _, s := range t.dispatchedNodes {
		items = append(items, &ObjWithTaskStatus{status: s.status})
	}
	t.status = checkStatus(t.status, items)
	return t.status
}




// Budget Negotiation Response Cache

type BudgetNegotiationResponse struct {
	AvailableNodes 	int64 			`json:"availableNodes,omitempty"`
	CDF 			*histogram.CDF 	`json:"cdf,omitempty"`
	TaskKey 		string 			`json:"taskId,omitempty"`
	Node            *kernel.Node    `json:"node,omitempty"`
}

type BudgetNegotiationResponseCacheItem struct {
	Neighbor 			*kernel.Node
	RequestSentAt 		time.Time
	ResponseArriveAt 	time.Time
	Response 			*BudgetNegotiationResponse
	IsDone              bool
}

type BudgetNegotiationResponseCache struct {
	Responses map[string]*BudgetNegotiationResponseCacheItem
	mutex *sync.Mutex
}

func NewBudgetNegotiationResponseCache() *BudgetNegotiationResponseCache {
	return &BudgetNegotiationResponseCache{
		Responses: make(map[string]*BudgetNegotiationResponseCacheItem),
		mutex: &sync.Mutex{},
	}
}

func (c *BudgetNegotiationResponseCache) Lock() {
	c.mutex.Lock()
}

func (c *BudgetNegotiationResponseCache) Unlock() {
	c.mutex.Unlock()
}


func (c *BudgetNegotiationResponseCache) SetResponse(neighbor *kernel.Node, response *BudgetNegotiationResponse) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if cacheItem, e := c.Responses[neighbor.Key()]; !e {
		c.Responses[neighbor.Key()] = &BudgetNegotiationResponseCacheItem{
			Neighbor: 	neighbor,
			Response: 	response,
			IsDone: 	true,
			ResponseArriveAt: time.Now(),
		}
	} else {
		cacheItem.Response = response
		cacheItem.IsDone = true
		cacheItem.ResponseArriveAt = time.Now()
	}
}


