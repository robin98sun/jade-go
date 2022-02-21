// reference: https://medium.com/rungo/unit-testing-made-easy-in-go-25077669318

package scheduler

import (
	"log"
	"testing"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"math"
	"sort"
)

func gen_random_list(list_size int) []float64 {
	result := make([]float64, list_size)
	for i := 0; i < list_size; i++ {
		result[i] = math.Round(rand.ExpFloat64() * float64(list_size))
	}
	return result
}

var SAMPLE_LIST []float64 = []float64{
	134, 3693, 1612, 2033, 1762, 669, 296, 567, 547, 935,
	2842, 3135, 645, 4265, 2267, 1170, 399, 635, 2153, 1836,
	348, 2672, 5318, 1662, 6104, 1057, 2900, 2777, 3715, 9208,
	2231, 387, 1181, 1063, 3092, 478, 2039, 781, 11764, 591, 
	271, 1061, 3182, 1470, 4686, 1077, 1997, 2430, 18210, 2618,
}


func TestScheduler_InsertHistogramItem(t *testing.T) {
	// random_list := gen_random_list(3000)
	random_list := SAMPLE_LIST
	log.Printf("sample list length: %v", len(random_list))

	var root *HistogramItem
	for _, v := range random_list {
		// log.Printf("inerting: %v \n", v)
		if root == nil {
			root = NewHistogramItem(v)
		} else {
			root = root.Insert(v).GetRoot()
		}
	}

	assert.Equal(t, int64(7), root.Height, "height of root should be exactly 7")
	assert.Equal(t, int64(50), root.Count, "count of nodes should be exactly 50")
	assert.Equal(t, float64(1612), root.Value, "value of root should be exactly 1762")

	smallest := root
	for ; smallest.Left != nil; smallest = smallest.Left {}

	largest := root
	for ; largest.Right != nil; largest = largest.Right {}

	assert.Equal(t, smallest.Height, int64(1), "height of smallest should be exactly 1")
	assert.Equal(t, largest.Height, int64(1), "height of largest should be exactly 1")

	assert.Nil(t, smallest.Smaller, "smaller of smallest should be exactly nil")
	assert.Nil(t, largest.Larger, "larger of largest should be exactly nil")


	assert.Equal(t, smallest.Value, float64(134), "value of smallest should be exactly 134")
	assert.Equal(t, largest.Value, float64(18210), "value of largest should be exactly 18210")

	
	sorted_sample_list := make([]float64, len(SAMPLE_LIST))
	copy(sorted_sample_list, SAMPLE_LIST)
	sort.Float64Slice(sorted_sample_list).Sort()
	p := smallest
	i := 0
	for ; p != nil; p = p.Larger {
		assert.Equal(t, p.Value, sorted_sample_list[i], "list should be sorted")
		i++
	}
}

func TestScheduler_DeleteHistogramItem(t *testing.T) {
	random_list := SAMPLE_LIST

	var root *HistogramItem
	for _, v := range random_list {
		// log.Printf("inerting: %v \n", v)
		if root == nil {
			root = NewHistogramItem(v)
		} else {
			root = root.Insert(v).GetRoot()
		}
	}

	for k := range random_list {
		
		i := len(random_list) - k - 1
		v := random_list[i]
		log.Printf("deleting: %v, %v \n", i, v)
		n := root.Find(v)
		assert.NotNil(t, n, "node should not be nil")

		replaced := n.Delete()
		if n == root && replaced != nil {
			root = replaced.GetRoot()
		} else if n == root && replaced == nil {
			assert.Equal(t, len(random_list)-1, k, "when root is nil it should be the last element")
		}

		log.Printf("root: %v", root.Describe())

		if k == len(random_list) - 1 {
			assert.Equal(t, n.Value, root.Value, "when deleting the last element, it should be root")
			assert.Nil(t, replaced, "when deleted the last element, there should have no other nodes left")
		} else {
			assert.Equal(t, int64(i), root.Count, "the remaining nodes after deleting should decreased by 1")
		}


	}
}

