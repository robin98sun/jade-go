package scheduler
import (
	"testing"
)


func TestInitiating(t *testing.T) {
	taskCache := NewTaskCache()
	podCache := NewPodCache()

	if taskCache == nil || podCache == nil {
		t.Errorf("initiating of scheduler failed")
	}
}

func BenchmarkInitiating(b *testing.B) {
	for i:=0; i<b.N; i++ {
		NewTaskCache()
		NewPodCache()
	}
}
