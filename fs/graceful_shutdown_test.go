package fs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/fserrors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGracefulShutdown(t *testing.T) {
	ctx, ci := AddConfig(context.Background())
	ci.TransferTimeout = Duration(time.Second)
	ctx, shutdown := WithGracefulShutdown(ctx)
	input, cancelInput := WithGracefulInput(ctx)
	defer cancelInput()
	active, ok := StartTransfer(input)
	require.True(t, ok)
	active, cancel := WithTransferTimeout(active)
	defer cancel()
	deadline, _ := active.Deadline()
	require.True(t, shutdown.Stop())
	require.False(t, shutdown.Stop())
	select {
	case <-input.Done():
	case <-time.After(time.Second):
		t.Fatal("input did not stop")
	}
	assert.NoError(t, active.Err())
	assert.NoError(t, ctx.Err())
	assert.True(t, IsGracefulStop(input, input.Err()))
	assert.False(t, IsGracefulStop(active, context.Canceled))
	counted := fserrors.FsError(context.Canceled)
	fserrors.Count(counted)
	assert.False(t, IsGracefulStop(input, counted), "already counted transfer failures must be preserved")
	_, ok = StartTransfer(ctx)
	assert.False(t, ok)
	nested, ok := StartTransfer(active)
	require.True(t, ok, "nested copies belong to the admitted object")
	nested, cancelNested := WithTransferTimeout(nested)
	defer cancelNested()
	nestedDeadline, _ := nested.Deadline()
	assert.Equal(t, deadline, nestedDeadline)
}

func TestGracefulShutdownStartRace(t *testing.T) {
	for range 100 {
		ctx, shutdown := WithGracefulShutdown(context.Background())
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				active, ok := StartTransfer(ctx)
				<-shutdown.Done()
				if ok {
					_, nestedOK := StartTransfer(active)
					assert.True(t, nestedOK)
				}
				_, ok = StartTransfer(ctx)
				assert.False(t, ok)
			})
		}
		shutdown.Stop()
		wg.Wait()
	}
}
