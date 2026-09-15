package fs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/fserrors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransferTimeoutContext(t *testing.T) {
	parent, ci := AddConfig(context.Background())
	ci.TransferTimeout = Duration(30 * time.Millisecond)
	ctx, cancel := WithTransferTimeout(parent)
	defer cancel()
	nested, nestedCancel := WithTransferTimeout(ctx)
	nestedCancel()
	require.Same(t, ctx, nested)
	require.NoError(t, ctx.Err())
	child, childCancel := context.WithCancel(ctx)
	childCancel()
	assert.NoError(t, TransferError(child, nil), "局部取消不能耗尽整个对象预算")
	<-ctx.Done()
	err := TransferError(ctx, errors.New("blocked GET"))
	assert.ErrorIs(t, err, ErrorTransferTimeout)
	assert.Contains(t, err.Error(), "30ms")
	assert.Contains(t, err.Error(), "blocked GET")
	assert.True(t, fserrors.IsNoRetryError(err))
	assert.True(t, fserrors.IsNoLowLevelRetryError(err))
	assert.False(t, fserrors.IsFatalError(err))
	assert.False(t, fserrors.ShouldRetry(err))
	assert.Equal(t, err, TransferError(ctx, err))
	assert.NoError(t, parent.Err())

	cleanup, stop := TransferCleanupContext(ctx)
	defer stop()
	assert.NoError(t, cleanup.Err())
	d, ok := cleanup.Deadline()
	require.True(t, ok)
	assert.LessOrEqual(t, time.Until(d), 30*time.Second)
	second, stopSecond := TransferCleanupContext(ctx)
	defer stopSecond()
	d2, _ := second.Deadline()
	assert.Equal(t, d, d2, "清理重试不能重置预算")
}

func TestTransferTimeoutParentCancellation(t *testing.T) {
	for _, useDeadline := range []bool{false, true} {
		parent, ci := AddConfig(context.Background())
		ci.TransferTimeout = Duration(time.Second)
		var stop context.CancelFunc
		if useDeadline {
			parent, stop = context.WithTimeout(parent, 10*time.Millisecond)
		} else {
			parent, stop = context.WithCancel(parent)
		}
		ctx, cancel := WithTransferTimeout(parent)
		if !useDeadline {
			stop()
		}
		<-ctx.Done()
		err := TransferError(ctx, nil)
		assert.ErrorIs(t, err, parent.Err())
		assert.NotErrorIs(t, err, ErrorTransferTimeout)
		cancel()
		stop()
	}
}

func TestTransferTimeoutConfig(t *testing.T) {
	for _, timeout := range []Duration{0, DurationOff} {
		ctx, ci := AddConfig(context.Background())
		ci.TransferTimeout = timeout
		got, cancel := WithTransferTimeout(ctx)
		cancel()
		assert.Same(t, ctx, got)
		assert.False(t, HasTransferTimeout(got))
	}
	ctx, ci := AddConfig(context.Background())
	ci.TransferTimeout = -1
	assert.ErrorContains(t, ci.Reload(ctx), "transfer-timeout")
}
