// reference: https://medium.com/rungo/unit-testing-made-easy-in-go-25077669318

package histogram

import (
	// "log"
	"testing"
	"github.com/stretchr/testify/assert"
	// "math/rand"
	// "sort"
)

func TestScheduler_CreateHistogram(t *testing.T) {
	sample_size := 1000000
	window_size := 100000
	list := gen_random_list_float(sample_size, float64(10))
	assert.Equal(t, len(list), sample_size, "random util should work")

	histogram := NewHistogram(int64(window_size), float64(10), 2)
	assert.NotNil(t, histogram, "histogram should not be nil")

	for i:=0; i<len(list); i++ {
		v := list[i]
		// log.Printf("original %v value: %v", i, v)
		histogram.Enqueue(v)
	}
	assert.Equal(t, int64(window_size), histogram.Count, "histogram size should equal window size")

	assert.NotNil(t, histogram.RootItem, "root item should not be nil")

	assert.Equal(t, int64(window_size), histogram.RootItem.Count, "histogram root count should equal window size")

	assert.Equal(t, int64(window_size), int64(len(histogram.Queue)), "histogram queue length should equal window size")

	bucketCount := int64(0)
	for i:=0; i<len(histogram.BucketHistogram.SubBucketHistograms); i++ {
		sbh := histogram.BucketHistogram.SubBucketHistograms[i]
		if sbh != nil {
			for j:=0; j<len(sbh.BucketList);j++ {
				bucketItem := sbh.BucketList[j]
				if bucketItem != nil {
					bucketCount+=bucketItem.Duplications
					// log.Printf(" i: %v, j: %v, value: %v, duplication: %v, count: %v", i, j, bucketItem.Value, bucketItem.Duplications, bucketItem.Count)
				}
			}
		}
	}

	assert.Equal(t, int64(window_size), bucketCount, "histogram queue length should equal window size")

}
