// reference: https://medium.com/rungo/unit-testing-made-easy-in-go-25077669318

package scheduler

import (
	"log"
	"strconv"
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
	// "fmt"
	// "uta.edu/aces/jade-go/kernel"
	ds "uta.edu/aces/jadesdk/data_structure"
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
	QueueType ds.TaskQueuingMechanism
	MaximumQueueingTime int64
	EstimatedServiceTime float64
	Priority int
}

func TestScheduler_Enqueuing_FIFO(t *testing.T) {
	q := NewSTQueue()
	for i := 0; i<100; i++ {
		p := &Payload{
			EnqueueingTimestamp: time.Now(),
			Key: genKey(10),
			TaskKey: genKey(10), 
			SubtaskKey: genKey(10),
			QueueType: ds.TaskQueuingFIFO,
		}
		q.Enqueue(
			STQueueTypeMain,
			p.Key, p.TaskKey, p.SubtaskKey, p, p.QueueType,
			0, 0, 0,
			log.Printf,
		)
	}
	assert.Equal(t, len(q.MainQueue), 100, "queue length should be exactly 100")
}

func TestScheduler_Dequeuing_FIFO(t *testing.T) {
	q := NewSTQueue()
	for i := 0; i<100; i++ {
		p := &Payload{
			EnqueueingTimestamp: time.Now(),
			Key: genKey(10),
			TaskKey: genKey(10), 
			SubtaskKey: genKey(10),
			QueueType: ds.TaskQueuingFIFO,
		}
		q.Enqueue(
			STQueueTypeMain,
			p.Key, p.TaskKey, p.SubtaskKey, p, p.QueueType,
			0, 0, 0,
			log.Printf,
		)
	}
	assert.Equal(t, len(q.MainQueue), 100, "queue length should be exactly 100")

	var previous_p *STQueueItem = nil
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
	assert.Equal(t, len(q.MainQueue), 81, "queue length should be exactly 81 after dequeuing 19 items")

}

func TestScheduler_Queueing_DDL_AND_PRQ(t *testing.T) {
	intervals := []int64{ 
		0, 660, 10, 13, 1, 20, 3, 9, 56, 209,
	}
	budgets_and_priorities_before_enqueuing := []int64{
		1000, 1000, 1000, 1000, 1000, 300, 300, 1000, 1000, 300,
	}
	budgets_after_enqueuing := []int64{
		1000, 300, 300, 300, 1000, 1000, 1000, 1000, 1000, 1000, 
	}
	priorities_after_enqueuing := []int{
		300, 300, 300, 1000, 1000, 1000, 1000, 1000, 1000, 1000,
	}
	preemptions_ddl := []int{
		0,0,0,0,0,
		4,4,0,0,6,
	}
	idx_after_enqueuing_ddl := []int{
		0,1,2,3,4,
		1,2,7,8,3,
	}
	preemptions_prq := []int{
		0,0,0,0,0,
		5,5,0,0,7,
	}
	idx_after_enqueuing_prq := []int{
		0,1,2,3,4,
		0,1,7,8,2,
	}
	for _, queueType := range []string{"ddl", "prq"} {
		q := NewSTQueue()
		for i := 0; i<len(intervals); i++ {
			interval := intervals[i]
			budget := budgets_and_priorities_before_enqueuing[i]
			if queueType == "ddl" {
				time.Sleep(time.Duration(interval)*time.Millisecond)
			}
			p := &Payload{
				EnqueueingTimestamp: time.Now(),
				Key: genKey(10),
				TaskKey: genKey(10), 
				SubtaskKey: genKey(10),
				QueueType: ds.TaskQueuingMechanism(queueType),
				MaximumQueueingTime: budget,
				Priority: int(budget),
			}
			_, queue_item, idx := q.Enqueue(
				STQueueTypeMain,
				p.Key, p.TaskKey, p.SubtaskKey, p, p.QueueType,
				float64(p.MaximumQueueingTime), p.Priority, 0, 
				// log.Printf,
				log.Printf,
			)
			// log.Println(queue_item.Deadline, queue_item.Budget, idx, len(q.Queue), queue_item.AmountPreempted)
			preemption_list := preemptions_prq
			if queueType == "ddl" {
				preemption_list = preemptions_ddl
			}
			idx_after_enqueuing_list := idx_after_enqueuing_prq
			if queueType == "ddl" {
				idx_after_enqueuing_list = idx_after_enqueuing_ddl
			}
			assert.Equal(
				t, 
				preemption_list[i], 
				queue_item.AmountPreempted, 
				"the preemption should be "+strconv.Itoa(preemption_list[i])+", but actual is "+strconv.Itoa(queue_item.AmountPreempted)+", for item["+strconv.Itoa(i)+"], queue type: " + queueType,
			)
			assert.Equal(
				t, 
				idx_after_enqueuing_list[i], 
				idx, 
				"the index should be "+strconv.Itoa(idx_after_enqueuing_list[i])+", but actual is "+strconv.Itoa(idx)+", for item["+strconv.Itoa(i)+"], queue type: " + queueType,
			)
		}
		assert.Equal(t, len(q.MainQueue), len(budgets_and_priorities_before_enqueuing), "queue length should be exactly 10")

		queue_item := q.Dequeue(nil)
		idx := 0
		var pre_item *STQueueItem = nil
		for queue_item != nil {
			budget := budgets_after_enqueuing[idx]
			priority := priorities_after_enqueuing[idx]
			if queueType == "ddl" {
				assert.Equal(
					t, 
					float64(budget), 
					queue_item.Budget, 
					"the budget should be that value",
				)
			} else {
				assert.Equal(
					t, 
					priority, 
					queue_item.Priority, 
					"the priority should be that value",
				)
			}
			
			if queueType == "ddl" {
				if pre_item != nil && pre_item.Budget == queue_item.Budget {
					assert.Greater(
						t, 
						int64((queue_item.Payload.(*Payload)).EnqueueingTimestamp.Sub(
							(pre_item.Payload.(*Payload)).EnqueueingTimestamp,
						)), 
						int64(0),
						"the later one should be younger than previous one at index["+strconv.Itoa(idx)+"]",
					)
				}
			} else {
				if pre_item != nil && pre_item.Priority == queue_item.Priority {
					assert.Greater(
						t, 
						int64((queue_item.Payload.(*Payload)).EnqueueingTimestamp.Sub(
							(pre_item.Payload.(*Payload)).EnqueueingTimestamp,
						)), 
						int64(0),
						"the later one should be younger than previous one at index["+strconv.Itoa(idx)+"], previous priority: " + strconv.Itoa(pre_item.Priority) + ", the later one priority: "+strconv.Itoa(queue_item.Priority),
					)
				}
			}

			idx += 1
			pre_item = queue_item
			queue_item = q.Dequeue(nil)
		}
		assert.Equal(t, idx, len(budgets_and_priorities_before_enqueuing), "queue length should be exactly 10")
	}
}

