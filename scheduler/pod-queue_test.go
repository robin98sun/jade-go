// reference: https://medium.com/rungo/unit-testing-made-easy-in-go-25077669318

package scheduler

import (
	// "log"
	// "strconv"
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
	// "fmt"
	"uta.edu/aces/jade-go/kernel"
	"math/rand"
)

func genKey(keyLen int) string {
	key := ""
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	for i := 0; i<keyLen; i++ {
		key += string(charset[rand.Intn(len(charset))])
	}
	return key
}

type Payload struct {
	EnqueueingTimestamp time.Time
	Key string
	TaskKey string
	SubtaskKey string
	QueueType kernel.TaskQueuingMechanism
	MaximumQueueingTime int64
	EstimatedServiceTime float64
}

func TestScheduler_Enqueuing_FIFO(t *testing.T) {
	q := NewPodQueue()
	for i := 0; i<100; i++ {
		p := &Payload{
			EnqueueingTimestamp: time.Now(),
			Key: genKey(10),
			TaskKey: genKey(10), 
			SubtaskKey: genKey(10),
			QueueType: kernel.TaskQueuingFIFO,
		}
		q.Enqueue(
			p.Key, p.TaskKey, p.SubtaskKey, p, p.QueueType,
			0, 0, 
			nil,
		)
	}
	assert.Equal(t, len(q.Queue), 100, "queue length should be exactly 100")
}

func TestScheduler_Dequeuing_FIFO(t *testing.T) {
	q := NewPodQueue()
	for i := 0; i<100; i++ {
		p := &Payload{
			EnqueueingTimestamp: time.Now(),
			Key: genKey(10),
			TaskKey: genKey(10), 
			SubtaskKey: genKey(10),
			QueueType: kernel.TaskQueuingFIFO,
		}
		q.Enqueue(
			p.Key, p.TaskKey, p.SubtaskKey, p, p.QueueType,
			0, 0, 
			nil,
		)
	}
	assert.Equal(t, len(q.Queue), 100, "queue length should be exactly 100")

	var previous_p *PodQueueItem = nil
	for i := 0; i<19; i++ {
		if previous_p == nil {
			previous_p = q.Dequeue(nil)
		} else {
			p := q.Dequeue(nil)
			assert.Greater(
				t, 
				int64((p.Payload.(*Payload)).EnqueueingTimestamp.Sub(
					(previous_p.Payload.(*Payload)).EnqueueingTimestamp,
				)), 
				int64(0),
				"the later one should be younger than previous one",
			)
		}
	}
	assert.Equal(t, len(q.Queue), 81, "queue length should be exactly 81 after dequeuing 19 items")

}

func TestScheduler_Queueing_DDL(t *testing.T) {
	intervals := []int64{
		0,
		100,
		10,
		13,
		1,
		20,
		3,
		9,
		56,
		209,
	}
	budgets := []int64{
		10000,
		10000,
		10000,
		10000,
		10000,
		3000,
		3000,
		10000,
		10000,
		3000,
	}
	preemptions := []int{
		0,0,0,0,0,
		5,5,0,0,7,
	}
	idx_after_enqueuing := []int{
		0,1,2,3,4,
		0,1,7,8,2,
	}
	q := NewPodQueue()
	for i := 0; i<len(intervals); i++ {
		interval := intervals[i]
		budget := budgets[i]
		time.Sleep(time.Duration(interval)*time.Millisecond)
		p := &Payload{
			EnqueueingTimestamp: time.Now(),
			Key: genKey(10),
			TaskKey: genKey(10), 
			SubtaskKey: genKey(10),
			QueueType: kernel.TaskQueuingDDL,
			MaximumQueueingTime: budget,
		}
		_, queue_item, idx := q.Enqueue(
			p.Key, p.TaskKey, p.SubtaskKey, p, p.QueueType,
			p.MaximumQueueingTime, 0, 
			// log.Printf,
			nil,
		)
		// log.Println(queue_item.Deadline, queue_item.Budget, idx, len(q.Queue), queue_item.AmountPreempted)
		assert.Equal(
			t, 
			queue_item.AmountPreempted, 
			preemptions[i], 
			"the preemption should be that value",
		)
		assert.Equal(
			t, 
			idx, 
			idx_after_enqueuing[i], 
			"the index should be that value",
		)
	}
	assert.Equal(t, len(q.Queue), len(budgets), "queue length should be exactly 10")

	queue_item := q.Dequeue(nil)
	idx := 0
	var pre_item *PodQueueItem = nil
	for queue_item != nil {
		budget := int64(10000)
		if idx < 3 {
			budget = int64(3000)
		}
		assert.Equal(
			t, 
			queue_item.Budget, 
			budget, 
			"the budget should be that value",
		)
		if pre_item != nil && pre_item.Budget == queue_item.Budget {
			assert.Greater(
				t, 
				int64((queue_item.Payload.(*Payload)).EnqueueingTimestamp.Sub(
					(pre_item.Payload.(*Payload)).EnqueueingTimestamp,
				)), 
				int64(0),
				"the later one should be younger than previous one",
			)
		}

		idx += 1
		pre_item = queue_item
		queue_item = q.Dequeue(nil)
	}
	assert.Equal(t, idx, len(budgets), "queue length should be exactly 10")
}
