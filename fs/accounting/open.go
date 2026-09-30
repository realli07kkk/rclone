package accounting

import (
	"sync"
	"time"
)

// OpenStats 记录首个成功的 source Open 调用耗时，零值可用，允许并发访问。
// 耗时包含该次调用内部的重试，不包含正文传输或后续重新打开。
type OpenStats struct {
	mu       sync.Mutex
	duration time.Duration
	opened   bool
}

// RecordOpenDuration 记录首个成功的 Open 调用耗时，后续调用不覆盖它。
func (s *OpenStats) RecordOpenDuration(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.opened {
		s.duration, s.opened = d, true
	}
}

// OpenDuration 返回首个成功的 Open 调用耗时；尚未成功打开时 ok 为 false。
func (s *OpenStats) OpenDuration() (d time.Duration, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.duration, s.opened
}

type openDurationReader interface {
	OpenDuration() (time.Duration, bool)
}
