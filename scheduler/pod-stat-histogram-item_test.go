// reference: https://medium.com/rungo/unit-testing-made-easy-in-go-25077669318

package scheduler

import (
	// "log"
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

func sorted_list(list []float64) []float64{
	sorted_list := make([]float64, len(list))
	copy(sorted_list, list)
	sort.Float64Slice(sorted_list).Sort()
	return sorted_list
}

func create_tree(list []float64) *HistogramItem {
	var root *HistogramItem
	for _, v := range list {
		if root == nil {
			root = NewHistogramItem(v)
		} else {
			_, newRoot := root.Insert(v)
			if newRoot != nil {
				root = newRoot
			}
		}
	}
	return root
}

func delete_in_order(t *testing.T, list string, order string, size int, enforceOrdering bool) {
	random_list := SAMPLE_LIST

	sorted_sample_list := make([]float64, len(SAMPLE_LIST))
	copy(sorted_sample_list, SAMPLE_LIST)
	sort.Float64Slice(sorted_sample_list).Sort()

	if list == "sorted" {
		random_list = sorted_sample_list
	} else if list == "random" {
		random_list = gen_random_list(size)
	}

	var root *HistogramItem
	var smallest *HistogramItem
	var largest *HistogramItem
	min, max := float64(-1), float64(0)
	for _, v := range random_list {
		// log.Printf("inerting: [%v] %v \n", i, v)
		if root == nil {
			root = NewHistogramItem(v)
		} else {
			_, newRoot := root.Insert(v)
			if newRoot != nil {
				root = newRoot
			}
		}
		// log.Printf("after inserting: %v\n\n", root.Describe())
		if enforceOrdering {
			if min < 0 || v < min {
				min = v
			}
			if v > max {
				max = v
			}
			for smallest = root; smallest.Smaller != nil ; smallest = smallest.Smaller {}
			for largest = root; largest.Larger != nil; largest = largest.Larger{}
			assert.Equal(t, min, smallest.Value, "the smallest should be identical with the min in the list")
			assert.Equal(t, max, largest.Value, "the largest should be identical with the max in the list")
		}
	}

	// log.Printf("\n\nStart deleting [%v, %v, %v]\n\n", list, order, size)
	for k := range random_list {
		
		i := len(random_list) - k - 1
		remaining_count := i
		if order == "asc" {
			i = k
		}
		v := random_list[i]
		// log.Printf("\n\ndeleting: %v, %v \n", i, v)
		// log.Printf("before deleting, root: %v", root.Describe())
		n := root.Find(v)
		assert.NotNil(t, n, "node should not be nil")
		count := n.Duplications
		if n == nil {
			break
		}

		if k == len(random_list) - 1 {
			assert.Equal(t, root.Value, n.Value, "when deleted the last element, there should have no other nodes left")
			assert.Equal(t, root.Count, int64(1), "when deleted the last element, there should have no other nodes left")
			if root.Value != n.Value{
				break
			}
		}

		replaced, newRoot := n.Delete()
		if newRoot != nil || (replaced == nil && newRoot == nil) {
			root = newRoot
		}

		if n == root && replaced == nil {
			root = nil
			assert.Equal(t, len(random_list)-1, k, "when root is nil it should be the last element")
			if k != len(random_list) - 1 {
				break
			}
		}

		if root != nil {
			n = root.Find(v)
			if count == 1 {
				assert.Nil(t, n, "the node should not be searchable anymore after being deleted")
			} else {
				assert.NotNil(t, n, "the node should not be delete if it has multiple duplications")
			}
			if (count == 1 && n != nil) || (count != 1 && n == nil) {
				break
			}
			// log.Printf("after deleting, root: %v", root.Describe())
		}

		if k == len(random_list) - 1 {
			assert.Nil(t, replaced, "when deleted the last element, there should have no other nodes left")
			if replaced != nil{
				break
			}
		} else {
			assert.NotNil(t, root, "root should not be nil")
			assert.Equal(t, int64(remaining_count), root.Count, "the remaining nodes after deleting should decreased by 1")
			if root == nil || root.Count != int64(remaining_count) {
				break
			}
		}

		if root != nil && enforceOrdering {
			for smallest = root; smallest.Smaller != nil; smallest = smallest.Smaller{}
			previous := smallest
			for p := smallest.Larger; p != nil ; p = p.Larger {
				assert.Greater(t, p.Value, previous.Value, "the sequence should be in ascending order")
				previous = p
			}
		}
	}	
}

func randomlyInsertAndDelete(t *testing.T, size int, enforceOrdering bool) {
	var root *HistogramItem = nil

	random_action_list := gen_random_list(size)
	random_value_list := gen_random_list(size)

	action_avg := float64(0)
	for _, v := range random_action_list {
		action_avg += v
	}
	action_avg /= float64(len(random_action_list))

	inserted_value_list := []float64{}

	for i := 0; i<len(random_action_list); i++ {
		action := random_action_list[i]
		valueToInsert := random_value_list[i]
		if action > action_avg {
			if root == nil {
				root = NewHistogramItem(valueToInsert)
			} else {
				_, newRoot := root.Insert(valueToInsert)
				if newRoot != nil {
					root = newRoot
				}
			}
			inserted_value_list = append(inserted_value_list, valueToInsert)
		} else if root != nil && len(inserted_value_list) > 0 {
			idx_delete := rand.Intn(len(inserted_value_list))
			valueToDelete := inserted_value_list[idx_delete]
			n := root.Find(valueToDelete)
			assert.NotNil(t, n, "the node to delete should be guaranteed not nil")
			replaced, newRoot := n.Delete()
			if replaced == nil && newRoot == nil {
				root = nil
			} else if newRoot != nil {
				root = newRoot
			}

			inserted_value_list = append(inserted_value_list[0:idx_delete], inserted_value_list[idx_delete+1:]...)

			if root == nil {
				assert.Equal(t, 0, len(inserted_value_list), "there should not have anymore values left when root is nil")
			}
		}
		if root != nil {
			assert.Equal(t, int64(len(inserted_value_list)), root.Count, "the number of nodes should equal the number of elements in the list")
		}

		if root != nil && enforceOrdering {
			var smallest *HistogramItem
			for smallest = root; smallest.Smaller != nil; smallest = smallest.Smaller{}
			previous := smallest
			for p := smallest.Larger; p != nil ; p = p.Larger {
				assert.Greater(t, p.Value, previous.Value, "the sequence should be in ascending order")
				previous = p
			}
		}
	}
	if root != nil {
		assert.Equal(t, int64(len(inserted_value_list)), root.Count, "if there have some nodes left")
	} else {
		assert.Equal(t, 0, len(inserted_value_list), "there should not have any nodes left")
	}
}

var SAMPLE_LIST []float64 = []float64{
	134, 3693, 1612, 2033, 1762, 669, 296, 567, 547, 935,
	2842, 3135, 645, 4265, 2267, 1170, 399, 635, 2153, 1836,
	348, 2672, 5318, 1662, 6104, 1057, 2900, 2777, 3715, 9208,
	2231, 387, 1181, 1063, 3092, 478, 2039, 781, 11764, 591, 
	271, 1061, 3182, 1470, 4686, 1077, 1997, 2430, 18210, 2618,
}


func TestScheduler_InsertHistogramItem(t *testing.T) {
	random_list := SAMPLE_LIST

	root := create_tree(random_list)

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

	
	sorted_sample_list := sorted_list(SAMPLE_LIST)
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
	
	delete_in_order(t, "sorted", "asc", 0, true)
	delete_in_order(t, "sorted", "desc", 0, true)
	delete_in_order(t, "original", "asc", 0, true)
	delete_in_order(t, "original", "desc", 0, true)
	delete_in_order(t, "random", "asc", 10, true)
	delete_in_order(t, "random", "desc", 10, true)
	delete_in_order(t, "random", "asc", 500, true)
	delete_in_order(t, "random", "desc", 500, true)
	delete_in_order(t, "random", "asc", 3000, true)
	delete_in_order(t, "random", "desc", 3000, true)
	delete_in_order(t, "random", "desc", 100000, false)
	// delete_in_order(t, "random", "asc", 1000000, false)
	
}

func TestScheduler_RandomInsertDeleteHistogramItem(t *testing.T) {
	randomlyInsertAndDelete(t, 3000, true)
	randomlyInsertAndDelete(t, 10000, true)
	randomlyInsertAndDelete(t, 1000000, true)
}

func TestScheduler_FindNoLargerThan(t *testing.T) {
	is_something_wrong := false
	for round := 0; round < 10; round++ {
		if is_something_wrong {break}
		for size := 10; size <= 100000; size *= 10 {
			if is_something_wrong {break}
			random_sample_list := gen_random_list(size)
			sorted_sample_list := sorted_list(random_sample_list)
			
			root := create_tree(random_sample_list)

			pre := float64(-1)
			for i := 0; i < len(random_sample_list); i++ {
				v := sorted_sample_list[i] 
				if i > 0 && v == pre {continue}
				pre = v

				nlt := root.FindNoLargerThan(v)
				// log.Printf("to find value no larger than %v", v)
				assert.Equal(t, v, nlt.Value, "the no larger value should equal to itself value")
				if nlt == nil || v != nlt.Value {
					is_something_wrong = true
					break
				} else {
					// log.Printf("  ---found value no larger than %v: %v", v, nlt.Value)
				}

				v -= 0.1
				nlt = root.FindNoLargerThan(v)
				// log.Printf("to find value no larger than %v", v)
				if i == 0 || pre < 0 {
					assert.Nil(t, nlt, "it should not have smaller value when it is already the smallest value")
					if nlt != nil {
						is_something_wrong = true
						break
					}
				} else {
					assert.NotNil(t, nlt, "it should have smaller value when it is not the smallest value")
					assert.Equal(t, sorted_sample_list[i-1], nlt.Value, "the smaller value should exist and equal to previous value")
					if nlt == nil || nlt.Value != sorted_sample_list[i-1] {
						is_something_wrong = true
						break
					} else {
						// log.Printf("   ---found value no larger than %v: %v", v, nlt.Value)
					}
				}
			}
		}
	}
	
}

func TestScheduler_CumulativeCount(t *testing.T) {

	is_something_wrong := false
	for round := 0; round < 1; round++ {
		if is_something_wrong {break}
		for size := 10; size <= 1000000; size *= 10 {
			if is_something_wrong {break}
			random_sample_list := gen_random_list(size)
			sorted_sample_list := sorted_list(random_sample_list)
			root := create_tree(random_sample_list)
			pre := float64(-1)
			cc := int64(0)
			for i := 0; i < len(random_sample_list); i++ {
				v := sorted_sample_list[i] 
				if i > 0 && v == pre {continue}
				pre = v

				node := root.Find(v)
				cumulativeCount := node.CumulativeCount()

				cc += node.Duplications

				assert.Equal(t, cc, cumulativeCount, "cumulative count should equal with the sum")	

				if cc != cumulativeCount {
					is_something_wrong = true
					break
				}
				
			}
		}
	}
}



