package accounting

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAverageLoopStopsAfterLastCheck(t *testing.T) {
	ctx := context.Background()
	stats := NewStats(ctx)
	t.Cleanup(func() {
		stats.mu.Lock()
		defer stats.mu.Unlock()
		stats._stopAverageLoop()
	})
	transfer := stats.NewTransferRemoteSize("transfer", 0, nil, nil)
	firstCheck := stats.NewCheckingTransfer(mockobject.New("first"), "checking")
	lastCheck := stats.NewCheckingTransfer(mockobject.New("last"), "checking")

	transfer.Done(ctx, nil)
	firstCheck.Done(ctx, nil)
	assert.True(t, stats.average.started, "a check is still active")

	lastCheck.Done(ctx, nil)
	assert.False(t, stats.average.started, "completed checks must release the averaging goroutine")
	assert.Equal(t, int64(2), stats.GetChecks())
	assert.Equal(t, int64(1), stats.GetTransfers())
}

func TestETA(t *testing.T) {
	for _, test := range []struct {
		size, total int64
		rate        float64
		wantETA     time.Duration
		wantOK      bool
		wantString  string
	}{
		// Custom String Cases
		{size: 0, total: 365 * 86400, rate: 1.0, wantETA: 365 * 86400 * time.Second, wantOK: true, wantString: "1y"},
		{size: 0, total: 7 * 86400, rate: 1.0, wantETA: 7 * 86400 * time.Second, wantOK: true, wantString: "1w"},
		{size: 0, total: 1 * 86400, rate: 1.0, wantETA: 1 * 86400 * time.Second, wantOK: true, wantString: "1d"},
		{size: 0, total: 1110 * 86400, rate: 1.0, wantETA: 1110 * 86400 * time.Second, wantOK: true, wantString: "3y2w1d"},
		{size: 0, total: 15 * 86400, rate: 1.0, wantETA: 15 * 86400 * time.Second, wantOK: true, wantString: "2w1d"},
		// Composite Custom String Cases
		{size: 0, total: 1.5 * 86400, rate: 1.0, wantETA: 1.5 * 86400 * time.Second, wantOK: true, wantString: "1d12h"},
		{size: 0, total: 95000, rate: 1.0, wantETA: 95000 * time.Second, wantOK: true, wantString: "1d2h23m"}, // Short format, if full it would be "1d2h23m20s"
		// Standard Duration String Cases
		{size: 0, total: 1, rate: 2.0, wantETA: 0, wantOK: true, wantString: "0s"},
		{size: 0, total: 1, rate: 1.0, wantETA: time.Second, wantOK: true, wantString: "1s"},
		{size: 0, total: 1, rate: 0.5, wantETA: 2 * time.Second, wantOK: true, wantString: "2s"},
		{size: 0, total: 100, rate: 1.0, wantETA: 100 * time.Second, wantOK: true, wantString: "1m40s"},
		{size: 50, total: 100, rate: 1.0, wantETA: 50 * time.Second, wantOK: true, wantString: "50s"},
		{size: 100, total: 100, rate: 1.0, wantETA: 0 * time.Second, wantOK: true, wantString: "0s"},
		// No String Cases
		{size: -1, total: 100, rate: 1.0, wantETA: 0, wantOK: false, wantString: "-"},
		{size: 200, total: 100, rate: 1.0, wantETA: 0, wantOK: false, wantString: "-"},
		{size: 10, total: -1, rate: 1.0, wantETA: 0, wantOK: false, wantString: "-"},
		{size: 10, total: 20, rate: 0.0, wantETA: 0, wantOK: false, wantString: "-"},
		{size: 10, total: 20, rate: -1.0, wantETA: 0, wantOK: false, wantString: "-"},
		{size: 0, total: 0, rate: 1.0, wantETA: 0, wantOK: false, wantString: "-"},
		// Extreme Cases
		{size: 0, total: (1 << 63) - 1, rate: 1.0, wantETA: (time.Duration((1<<63)-1) / time.Second) * time.Second, wantOK: true, wantString: "-"},
		{size: 0, total: ((1 << 63) - 1) / int64(time.Second), rate: 1.0, wantETA: (time.Duration((1<<63)-1) / time.Second) * time.Second, wantOK: true, wantString: "-"},
		{size: 0, total: ((1<<63)-1)/int64(time.Second) - 1, rate: 1.0, wantETA: (time.Duration((1<<63)-1)/time.Second - 1) * time.Second, wantOK: true, wantString: "292y24w3d"}, // Short format, if full it would be "292y24w3d23h47m15s"
		{size: 0, total: ((1<<63)-1)/int64(time.Second) - 1, rate: 0.1, wantETA: (time.Duration((1<<63)-1) / time.Second) * time.Second, wantOK: true, wantString: "-"},
	} {
		t.Run(fmt.Sprintf("size=%d/total=%d/rate=%f", test.size, test.total, test.rate), func(t *testing.T) {
			gotETA, gotOK := eta(test.size, test.total, test.rate)
			assert.Equal(t, int64(test.wantETA), int64(gotETA))
			assert.Equal(t, test.wantOK, gotOK)
			gotString := etaString(test.size, test.total, test.rate)
			assert.Equal(t, test.wantString, gotString)
		})
	}
}

func TestPercentage(t *testing.T) {
	assert.Equal(t, percent(0, 1000), "0%")
	assert.Equal(t, percent(1, 1000), "0%")
	assert.Equal(t, percent(9, 1000), "1%")
	assert.Equal(t, percent(500, 1000), "50%")
	assert.Equal(t, percent(1000, 1000), "100%")
	assert.Equal(t, percent(1e8, 1e9), "10%")
	assert.Equal(t, percent(1e8, 1e9), "10%")
	assert.Equal(t, percent(0, 0), "-")
	assert.Equal(t, percent(100, -100), "-")
	assert.Equal(t, percent(-100, 100), "-")
	assert.Equal(t, percent(-100, -100), "-")
}

func TestStatsError(t *testing.T) {
	ctx := context.Background()
	s := NewStats(ctx)
	assert.Equal(t, int64(0), s.GetErrors())
	assert.False(t, s.HadFatalError())
	assert.False(t, s.HadRetryError())
	assert.Equal(t, time.Time{}, s.RetryAfter())
	assert.Equal(t, nil, s.GetLastError())
	assert.False(t, s.Errored())

	t0 := time.Now()
	t1 := t0.Add(time.Second)

	_ = s.Error(nil)
	assert.Equal(t, int64(0), s.GetErrors())
	assert.False(t, s.HadFatalError())
	assert.False(t, s.HadRetryError())
	assert.Equal(t, time.Time{}, s.RetryAfter())
	assert.Equal(t, nil, s.GetLastError())
	assert.False(t, s.Errored())

	_ = s.Error(io.EOF)
	assert.Equal(t, int64(1), s.GetErrors())
	assert.False(t, s.HadFatalError())
	assert.True(t, s.HadRetryError())
	assert.Equal(t, time.Time{}, s.RetryAfter())
	assert.Equal(t, io.EOF, s.GetLastError())
	assert.True(t, s.Errored())

	e := fserrors.ErrorRetryAfter(t0)
	_ = s.Error(e)
	assert.Equal(t, int64(2), s.GetErrors())
	assert.False(t, s.HadFatalError())
	assert.True(t, s.HadRetryError())
	assert.Equal(t, t0, s.RetryAfter())
	assert.Equal(t, e, s.GetLastError())

	err := fmt.Errorf("potato: %w", fserrors.ErrorRetryAfter(t1))
	err = s.Error(err)
	assert.Equal(t, int64(3), s.GetErrors())
	assert.False(t, s.HadFatalError())
	assert.True(t, s.HadRetryError())
	assert.Equal(t, t1, s.RetryAfter())
	assert.Equal(t, t1, fserrors.RetryAfterErrorTime(err))

	_ = s.Error(fserrors.FatalError(io.EOF))
	assert.Equal(t, int64(4), s.GetErrors())
	assert.True(t, s.HadFatalError())
	assert.True(t, s.HadRetryError())
	assert.Equal(t, t1, s.RetryAfter())

	s.ResetErrors()
	assert.Equal(t, int64(0), s.GetErrors())
	assert.False(t, s.HadFatalError())
	assert.False(t, s.HadRetryError())
	assert.Equal(t, time.Time{}, s.RetryAfter())
	assert.Equal(t, nil, s.GetLastError())
	assert.False(t, s.Errored())

	_ = s.Error(fserrors.NoRetryError(io.EOF))
	assert.Equal(t, int64(1), s.GetErrors())
	assert.False(t, s.HadFatalError())
	assert.False(t, s.HadRetryError())
	assert.Equal(t, time.Time{}, s.RetryAfter())
}

func TestStatsTotalDuration(t *testing.T) {
	ctx := context.Background()
	startTime := time.Now()
	time1 := startTime.Add(-40 * time.Second)
	time2 := time1.Add(10 * time.Second)
	time3 := time2.Add(10 * time.Second)
	time4 := time3.Add(10 * time.Second)

	t.Run("Single completed transfer", func(t *testing.T) {
		s := NewStats(ctx)
		tr1 := &Transfer{
			startedAt:   time1,
			completedAt: time2,
		}
		s.AddTransfer(tr1)

		s.mu.Lock()
		total := s._totalDuration()
		s.mu.Unlock()

		assert.Equal(t, 1, len(s.startedTransfers))
		assert.Equal(t, 10*time.Second, total)
		s.RemoveTransfer(tr1)
		assert.Equal(t, 10*time.Second, total)
		assert.Equal(t, 0, len(s.startedTransfers))
	})

	t.Run("Single uncompleted transfer", func(t *testing.T) {
		s := NewStats(ctx)
		tr1 := &Transfer{
			startedAt: time1,
		}
		s.AddTransfer(tr1)

		s.mu.Lock()
		total := s._totalDuration()
		s.mu.Unlock()

		assert.Equal(t, time.Since(time1)/time.Second, total/time.Second)
		s.RemoveTransfer(tr1)
		assert.Equal(t, time.Since(time1)/time.Second, total/time.Second)
	})

	t.Run("Overlapping without ending", func(t *testing.T) {
		s := NewStats(ctx)
		tr1 := &Transfer{
			startedAt:   time2,
			completedAt: time3,
		}
		s.AddTransfer(tr1)
		tr2 := &Transfer{
			startedAt:   time2,
			completedAt: time2.Add(time.Second),
		}
		s.AddTransfer(tr2)
		tr3 := &Transfer{
			startedAt:   time1,
			completedAt: time3,
		}
		s.AddTransfer(tr3)
		tr4 := &Transfer{
			startedAt:   time3,
			completedAt: time4,
		}
		s.AddTransfer(tr4)
		tr5 := &Transfer{
			startedAt: time.Now(),
		}
		s.AddTransfer(tr5)

		time.Sleep(time.Millisecond)

		s.mu.Lock()
		total := s._totalDuration()
		s.mu.Unlock()

		assert.Equal(t, time.Duration(30), total/time.Second)
		s.RemoveTransfer(tr1)
		assert.Equal(t, time.Duration(30), total/time.Second)
		s.RemoveTransfer(tr2)
		assert.Equal(t, time.Duration(30), total/time.Second)
		s.RemoveTransfer(tr3)
		assert.Equal(t, time.Duration(30), total/time.Second)
		s.RemoveTransfer(tr4)
		assert.Equal(t, time.Duration(30), total/time.Second)
	})

	t.Run("Mixed completed and uncompleted transfers", func(t *testing.T) {
		s := NewStats(ctx)
		s.AddTransfer(&Transfer{
			startedAt:   time1,
			completedAt: time2,
		})
		s.AddTransfer(&Transfer{
			startedAt: time2,
		})
		s.AddTransfer(&Transfer{
			startedAt: time3,
		})
		s.AddTransfer(&Transfer{
			startedAt: time3,
		})

		s.mu.Lock()
		total := s._totalDuration()
		s.mu.Unlock()

		assert.Equal(t, startTime.Sub(time1)/time.Second, total/time.Second)
	})
}

func TestRemoteStats(t *testing.T) {
	ctx := context.Background()
	startTime := time.Now()
	time1 := startTime.Add(-40 * time.Second)
	time2 := time1.Add(10 * time.Second)

	t.Run("Single completed transfer", func(t *testing.T) {
		s := NewStats(ctx)
		tr1 := &Transfer{
			startedAt:   time1,
			completedAt: time2,
		}
		s.AddTransfer(tr1)
		time.Sleep(time.Millisecond)
		rs, err := s.RemoteStats(false)

		require.NoError(t, err)
		assert.Equal(t, float64(10), rs["transferTime"])
		assert.Greater(t, rs["elapsedTime"], float64(0))
	})
}

// make time ranges from string description for testing
func makeTimeRanges(t *testing.T, in []string) timeRanges {
	trs := make(timeRanges, len(in))
	for i, Range := range in {
		var start, end int64
		n, err := fmt.Sscanf(Range, "%d-%d", &start, &end)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		trs[i] = timeRange{time.Unix(start, 0), time.Unix(end, 0)}
	}
	return trs
}

func (trs timeRanges) toStrings() (out []string) {
	out = []string{}
	for _, tr := range trs {
		out = append(out, fmt.Sprintf("%d-%d", tr.start.Unix(), tr.end.Unix()))
	}
	return out
}

func TestTimeRangeMerge(t *testing.T) {
	for _, test := range []struct {
		in   []string
		want []string
	}{{
		in:   []string{},
		want: []string{},
	}, {
		in:   []string{"1-2"},
		want: []string{"1-2"},
	}, {
		in:   []string{"1-4", "2-3"},
		want: []string{"1-4"},
	}, {
		in:   []string{"2-3", "1-4"},
		want: []string{"1-4"},
	}, {
		in:   []string{"1-3", "2-4"},
		want: []string{"1-4"},
	}, {
		in:   []string{"2-4", "1-3"},
		want: []string{"1-4"},
	}, {
		in:   []string{"1-2", "2-3"},
		want: []string{"1-3"},
	}, {
		in:   []string{"2-3", "1-2"},
		want: []string{"1-3"},
	}, {
		in:   []string{"1-2", "3-4"},
		want: []string{"1-2", "3-4"},
	}, {
		in:   []string{"1-3", "7-8", "4-6", "2-5", "7-8", "7-8"},
		want: []string{"1-6", "7-8"},
	}} {

		in := makeTimeRanges(t, test.in)
		in.merge()

		got := in.toStrings()
		assert.Equal(t, test.want, got)
	}
}

func TestTimeRangeCull(t *testing.T) {
	for _, test := range []struct {
		in           []string
		cutoff       int64
		want         []string
		wantDuration time.Duration
	}{{
		in:           []string{},
		cutoff:       1,
		want:         []string{},
		wantDuration: 0 * time.Second,
	}, {
		in:           []string{"1-2"},
		cutoff:       1,
		want:         []string{"1-2"},
		wantDuration: 0 * time.Second,
	}, {
		in:           []string{"2-5", "7-9"},
		cutoff:       1,
		want:         []string{"2-5", "7-9"},
		wantDuration: 0 * time.Second,
	}, {
		in:           []string{"2-5", "7-9"},
		cutoff:       4,
		want:         []string{"2-5", "7-9"},
		wantDuration: 0 * time.Second,
	}, {
		in:           []string{"2-5", "7-9"},
		cutoff:       5,
		want:         []string{"7-9"},
		wantDuration: 3 * time.Second,
	}, {
		in:           []string{"2-5", "7-9", "2-5", "2-5"},
		cutoff:       6,
		want:         []string{"7-9"},
		wantDuration: 9 * time.Second,
	}, {
		in:           []string{"7-9", "3-3", "2-5"},
		cutoff:       7,
		want:         []string{"7-9"},
		wantDuration: 3 * time.Second,
	}, {
		in:           []string{"2-5", "7-9"},
		cutoff:       8,
		want:         []string{"7-9"},
		wantDuration: 3 * time.Second,
	}, {
		in:           []string{"2-5", "7-9"},
		cutoff:       9,
		want:         []string{},
		wantDuration: 5 * time.Second,
	}, {
		in:           []string{"2-5", "7-9"},
		cutoff:       10,
		want:         []string{},
		wantDuration: 5 * time.Second,
	}} {

		in := makeTimeRanges(t, test.in)
		cutoff := time.Unix(test.cutoff, 0)
		gotDuration := in.cull(cutoff)

		what := fmt.Sprintf("in=%q, cutoff=%d", test.in, test.cutoff)
		got := in.toStrings()
		assert.Equal(t, test.want, got, what)
		assert.Equal(t, test.wantDuration, gotDuration, what)
	}
}

func TestTimeRangeDuration(t *testing.T) {
	assert.Equal(t, 0*time.Second, timeRanges{}.total())
	assert.Equal(t, 1*time.Second, makeTimeRanges(t, []string{"1-2"}).total())
	assert.Equal(t, 91*time.Second, makeTimeRanges(t, []string{"1-2", "10-100"}).total())
}

func TestPruneTransfers(t *testing.T) {
	ctx := context.Background()
	ci := fs.GetConfig(ctx)
	for _, test := range []struct {
		Name                     string
		Transfers                int
		Limit                    int
		ExpectedStartedTransfers int
	}{
		{
			Name:                     "Limited number of StartedTransfers",
			Limit:                    100,
			Transfers:                200,
			ExpectedStartedTransfers: 100 + ci.Transfers,
		},
		{
			Name:                     "Unlimited number of StartedTransfers",
			Limit:                    -1,
			Transfers:                200,
			ExpectedStartedTransfers: 200,
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			prevLimit := MaxCompletedTransfers
			MaxCompletedTransfers = test.Limit
			defer func() { MaxCompletedTransfers = prevLimit }()

			s := NewStats(ctx)
			for i := int64(1); i <= int64(test.Transfers); i++ {
				s.AddTransfer(&Transfer{
					startedAt:   time.Unix(i, 0),
					completedAt: time.Unix(i+1, 0),
				})
			}

			s.mu.Lock()
			assert.Equal(t, time.Duration(test.Transfers)*time.Second, s._totalDuration())
			assert.Equal(t, test.Transfers, len(s.startedTransfers))
			s.mu.Unlock()

			for range test.Transfers {
				s.PruneTransfers()
			}

			s.mu.Lock()
			assert.Equal(t, time.Duration(test.Transfers)*time.Second, s._totalDuration())
			assert.Equal(t, test.ExpectedStartedTransfers, len(s.startedTransfers))
			s.mu.Unlock()

		})
	}
}

func TestRemoveDoneTransfers(t *testing.T) {
	ctx := context.Background()
	s := NewStats(ctx)
	const transfers = 10
	for i := int64(1); i <= int64(transfers); i++ {
		s.AddTransfer(&Transfer{
			startedAt:   time.Unix(i, 0),
			completedAt: time.Unix(i+1, 0),
		})
	}

	s.mu.Lock()
	assert.Equal(t, time.Duration(transfers)*time.Second, s._totalDuration())
	assert.Equal(t, transfers, len(s.startedTransfers))
	s.mu.Unlock()

	s.RemoveDoneTransfers()

	s.mu.Lock()
	assert.Equal(t, time.Duration(transfers)*time.Second, s._totalDuration())
	assert.Equal(t, transfers, len(s.startedTransfers))
	s.mu.Unlock()
}

func TestDurationStats(t *testing.T) {
	summarize := func(durations []time.Duration) (latencyStats, bool) {
		var ds durationStats
		for _, d := range durations {
			ds.add(d)
		}
		return ds.latency()
	}
	t.Run("Empty", func(t *testing.T) {
		_, ok := summarize(nil)
		assert.False(t, ok)
	})

	t.Run("Single", func(t *testing.T) {
		ls, ok := summarize([]time.Duration{2 * time.Second})
		assert.True(t, ok)
		assert.Equal(t, latencyStats{
			count:   1,
			minimum: 2 * time.Second,
			average: 2 * time.Second,
			maximum: 2 * time.Second,
			p95:     2 * time.Second,
			p99:     2 * time.Second,
		}, ls)
	})

	t.Run("Unsorted", func(t *testing.T) {
		// durations 100ms..1ms in reverse order
		durations := make([]time.Duration, 100)
		for i := range durations {
			durations[i] = time.Duration(100-i) * time.Millisecond
		}
		ls, ok := summarize(durations)
		assert.True(t, ok)
		assert.Equal(t, 100, ls.count)
		assert.Equal(t, 1*time.Millisecond, ls.minimum)
		assert.Equal(t, 50*time.Millisecond+500*time.Microsecond, ls.average) // 5050ms/100
		assert.Equal(t, 100*time.Millisecond, ls.maximum)
		assert.GreaterOrEqual(t, ls.p95, 95*time.Millisecond)
		assert.Less(t, ls.p95, 95*time.Millisecond*65/64)
		assert.GreaterOrEqual(t, ls.p99, 99*time.Millisecond)
		assert.Less(t, ls.p99, 99*time.Millisecond*65/64)
	})
}

func TestTransferLatency(t *testing.T) {
	ctx := context.Background()
	s := NewStats(ctx)

	_, ok := s.transferLatency()
	assert.False(t, ok)
	assert.NotContains(t, s.String(), "Transfer times:")

	// successful transfers are recorded
	tr1 := s.NewTransferRemoteSize("potato", 0, nil, nil)
	time.Sleep(2 * time.Millisecond)
	tr1.Done(ctx, nil)

	// failed transfers are not recorded
	tr2 := s.NewTransferRemoteSize("sausage", 0, nil, nil)
	tr2.Done(ctx, io.EOF)

	ls, ok := s.transferLatency()
	assert.True(t, ok)
	assert.Equal(t, 1, ls.count)
	assert.GreaterOrEqual(t, ls.minimum, 2*time.Millisecond)
	assert.Equal(t, ls.minimum, ls.average)
	assert.Equal(t, ls.minimum, ls.maximum)

	// 没有新样本时结果保持不变。
	lsAgain, ok := s.transferLatency()
	assert.True(t, ok)
	assert.Equal(t, ls, lsAgain)

	assert.Contains(t, s.String(), "Transfer times:")

	rs, err := s.RemoteStats(false)
	require.NoError(t, err)
	require.Contains(t, rs, "transferTimes")
	times, ok := rs["transferTimes"].(rc.Params)
	require.True(t, ok)
	assert.Equal(t, 1, times["count"])

	s.ResetCounters()
	_, ok = s.transferLatency()
	assert.False(t, ok)
	assert.NotContains(t, s.String(), "Transfer times:")
}

func TestHeaderLatency(t *testing.T) {
	ctx := context.Background()
	s := NewStats(ctx)

	_, ok := s.headerLatency()
	assert.False(t, ok)
	assert.NotContains(t, s.String(), "Header times:")

	// successful transfers whose source was opened are recorded
	tr1 := s.NewTransferRemoteSize("potato", 0, nil, nil)
	tr1.Account(ctx, &timedSource{ReadCloser: io.NopCloser(strings.NewReader("potato")), duration: 2 * time.Millisecond, opened: true})
	tr1.Done(ctx, nil)

	// failed transfers are not recorded
	tr2 := s.NewTransferRemoteSize("sausage", 0, nil, nil)
	tr2.Account(ctx, &timedSource{ReadCloser: io.NopCloser(strings.NewReader("sausage")), duration: time.Second, opened: true})
	tr2.Done(ctx, io.EOF)

	// transfers whose source was never opened (e.g. server side copy
	// or dry run) are not recorded
	tr3 := s.NewTransferRemoteSize("bean", 0, nil, nil)
	tr3.Account(ctx, nil)
	tr3.Done(ctx, nil)

	ls, ok := s.headerLatency()
	assert.True(t, ok)
	assert.Equal(t, 1, ls.count)
	assert.GreaterOrEqual(t, ls.minimum, 2*time.Millisecond)
	assert.Equal(t, ls.minimum, ls.average)
	assert.Equal(t, ls.minimum, ls.maximum)

	// 没有新样本时结果保持不变。
	lsAgain, ok := s.headerLatency()
	assert.True(t, ok)
	assert.Equal(t, ls, lsAgain)

	assert.Contains(t, s.String(), "Header times:")

	rs, err := s.RemoteStats(false)
	require.NoError(t, err)
	require.Contains(t, rs, "headerTimes")
	times, ok := rs["headerTimes"].(rc.Params)
	require.True(t, ok)
	assert.Equal(t, 1, times["count"])

	s.ResetCounters()
	_, ok = s.headerLatency()
	assert.False(t, ok)
	assert.NotContains(t, s.String(), "Header times:")
}

type timedSource struct {
	io.ReadCloser
	duration time.Duration
	opened   bool
}

func (r *timedSource) OpenDuration() (time.Duration, bool) {
	return r.duration, r.opened
}

func TestTransferLatencyNested(t *testing.T) {
	ctx := context.Background()
	s := NewStats(ctx)
	outer := s.NewTransferRemoteSize("nested", 0, nil, nil)
	inner := s.NewTransferRemoteSize("nested", 0, nil, nil)
	inner.Done(ctx, nil)
	outer.Done(ctx, nil)
	ls, ok := s.transferLatency()
	require.True(t, ok)
	assert.Equal(t, int(s.GetTransfers()), ls.count)
	assert.Equal(t, 1, ls.count)
}

func TestHeaderLatencySourceTiming(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		t.Run(fmt.Sprint(lazy), func(t *testing.T) {
			ctx := context.Background()
			s := NewStats(ctx)
			r := &timedSource{ReadCloser: io.NopCloser(strings.NewReader("test")), duration: 100 * time.Millisecond, opened: !lazy}
			tr := s.NewTransferRemoteSize("test", 4, nil, nil)
			tr.Account(ctx, r)
			r.opened = true
			tr.Reset(ctx)
			tr.Account(ctx, &timedSource{ReadCloser: io.NopCloser(strings.NewReader("test")), duration: time.Second, opened: true})
			tr.Done(ctx, nil)
			ls, ok := s.headerLatency()
			require.True(t, ok)
			assert.Equal(t, 1, ls.count)
			assert.Equal(t, r.duration, ls.minimum)
			assert.Equal(t, r.duration, ls.maximum)
		})
	}
}

func TestHeaderLatencyWithoutSourceTiming(t *testing.T) {
	ctx := context.Background()
	s := NewStats(ctx)
	tr := s.NewTransferRemoteSize("pipe", 0, nil, nil)
	tr.Account(ctx, io.NopCloser(strings.NewReader("test")))
	tr.Done(ctx, nil)
	_, ok := s.headerLatency()
	assert.False(t, ok)
}

func TestDurationStatsBounded(t *testing.T) {
	var ds durationStats
	for i := range 100_000 {
		ds.add(time.Duration(i) * time.Millisecond)
	}
	assert.LessOrEqual(t, cap(ds.buckets), 4096)
	ls, ok := ds.latency()
	require.True(t, ok)
	assert.Equal(t, 100_000, ls.count)
	assert.Equal(t, 99_999*time.Millisecond/2, ls.average)
}

func TestDurationStatsMergeAndRange(t *testing.T) {
	var whole, left, right durationStats
	rng := rand.New(rand.NewSource(1))
	durations := []time.Duration{0, 1, 63, 64, 127, 128, 255, 256, math.MaxInt64}
	for range 10_000 {
		durations = append(durations, time.Duration(rng.Int63()))
	}
	for i, d := range durations {
		whole.add(d)
		if i%2 == 0 {
			left.add(d)
		} else {
			right.add(d)
		}
	}
	left.merge(&right)
	want, _ := whole.latency()
	slices.Sort(durations)
	for p, got := range map[int]time.Duration{95: want.p95, 99: want.p99} {
		exact := durations[(p*len(durations)+99)/100-1]
		assert.GreaterOrEqual(t, got, exact)
		assert.LessOrEqual(t, got-exact, exact/64)
	}
	got, ok := left.latency()
	require.True(t, ok)
	assert.Equal(t, want, got)
	right.reset()
	got, _ = left.latency()
	assert.Equal(t, want, got)
	assert.LessOrEqual(t, cap(whole.buckets), 4096)

	var large durationStats
	large.add(math.MaxInt64)
	large.add(math.MaxInt64)
	large.add(math.MaxInt64)
	ls, _ := large.latency()
	assert.Equal(t, time.Duration(math.MaxInt64), ls.average)
	assert.Equal(t, ls.maximum, ls.p99)

	for _, d := range durations {
		var ds durationStats
		// 加入更大的样本，避免 maximum 截断掩盖桶上界的误差。
		for range 100 {
			ds.add(d)
		}
		ds.add(math.MaxInt64)
		ls, _ := ds.latency()
		assert.GreaterOrEqual(t, ls.p95, d)
		assert.LessOrEqual(t, ls.p95-d, d/64)
		assert.GreaterOrEqual(t, ls.p99, d)
		assert.LessOrEqual(t, ls.p99-d, d/64)
	}
}

func TestLatencyConcurrentReset(t *testing.T) {
	s := NewStats(context.Background())
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				s.transferLatency()
				s.headerLatency()
			}
		})
	}
	for range 1000 {
		s.ResetCounters()
		s.AddTransferDuration(time.Second)
		s.AddHeaderTime(time.Millisecond)
	}
	wg.Wait()
	s.ResetCounters()
	s.AddTransferDuration(2 * time.Second)
	s.AddHeaderTime(2 * time.Millisecond)
	ls, _ := s.transferLatency()
	hs, _ := s.headerLatency()
	assert.Equal(t, 1, ls.count)
	assert.Equal(t, 2*time.Second, ls.average)
	assert.Equal(t, 1, hs.count)
	assert.Equal(t, 2*time.Millisecond, hs.average)
}

func BenchmarkDurationStats(b *testing.B) {
	for _, count := range []int{100, 1_000_000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			var ds durationStats
			for range count {
				ds.add(time.Second)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ds.latency()
			}
		})
	}
}
