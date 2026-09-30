package accounting

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpenStats(t *testing.T) {
	var timer OpenStats
	_, ok := timer.OpenDuration()
	assert.False(t, ok)
	timer.RecordOpenDuration(time.Millisecond)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			timer.RecordOpenDuration(time.Second)
			d, ok := timer.OpenDuration()
			assert.True(t, ok)
			assert.Equal(t, time.Millisecond, d)
		})
	}
	wg.Wait()
}
