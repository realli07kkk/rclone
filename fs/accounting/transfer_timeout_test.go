package accounting

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestTransferTimeoutBandwidth(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferTimeout = fs.Duration(30 * time.Millisecond)
	ctx, cancel := fs.WithTransferTimeout(ctx)
	defer cancel()
	stats := NewStats(ctx)
	acc := newAccountSizeName(ctx, stats, io.NopCloser(strings.NewReader("data")), 4, "slow")
	defer acc.Done()
	defer func() { require.NoError(t, acc.Close()) }()
	acc.tokenBucket[TokenBucketSlotAccounting] = rate.NewLimiter(1, 4)
	require.True(t, acc.tokenBucket[TokenBucketSlotAccounting].AllowN(time.Now(), 4))
	start := time.Now()
	_, err := io.Copy(io.Discard, acc)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond, "不能提前将 reservation 预测失败当成对象超时")
	assert.Less(t, time.Since(start), time.Second)
}

func TestTransferTimeoutStats(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferTimeout = fs.Duration(time.Millisecond)
	ctx, cancel := fs.WithTransferTimeout(ctx)
	defer cancel()
	<-ctx.Done()
	s := NewStats(ctx)
	err := s.Error(fs.TransferError(ctx, nil))
	s.Error(err)
	assert.EqualValues(t, 1, s.GetErrors())
	assert.True(t, s.HadTransferTimeout())
	assert.False(t, s.HadRetryError())
	s.Error(errors.New("another error"))
	assert.True(t, s.HadRetryError())
	assert.True(t, s.HadTransferTimeout(), "普通错误不能覆盖超时记录")
	sg := newStatsGroups()
	sg.set(ctx, "test", s)
	assert.True(t, sg.sum(ctx).HadTransferTimeout())
	s.ResetErrors()
	assert.False(t, s.HadTransferTimeout())
	s.Error(fs.TransferError(ctx, nil))
	s.ResetCounters()
	assert.False(t, s.HadTransferTimeout())
}
