package operations_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type slowOpenObject struct {
	fs.Object
	delay time.Duration
}

type timeoutPutFs struct {
	fs.Fs
	deadlines []time.Time
}

func (f *timeoutPutFs) Put(ctx context.Context, _ io.Reader, _ fs.ObjectInfo, _ ...fs.OpenOption) (fs.Object, error) {
	d, _ := ctx.Deadline()
	f.deadlines = append(f.deadlines, d)
	time.Sleep(20 * time.Millisecond)
	return nil, fserrors.RetryError(errors.New("retry upload"))
}

func TestCopyTransferTimeoutRetries(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferTimeout = fs.Duration(100 * time.Millisecond)
	ci.LowLevelRetries = 100
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
	require.NoError(t, err)
	dst.Features().Copy = nil
	srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	src := mockobject.New("file").WithContent([]byte("data"), mockobject.SeekModeNone)
	src.SetFs(srcFs)
	target := &timeoutPutFs{Fs: dst}
	_, err = operations.Copy(ctx, target, nil, "file", src)
	require.ErrorIs(t, err, fs.ErrorTransferTimeout)
	require.Greater(t, len(target.deadlines), 1)
	assert.Less(t, len(target.deadlines), ci.LowLevelRetries)
	for _, d := range target.deadlines {
		assert.Equal(t, target.deadlines[0], d)
	}
	assert.EqualValues(t, 1, accounting.Stats(ctx).GetErrors())
}

type timeoutHashObject struct{ fs.Object }

func (o timeoutHashObject) Hash(ctx context.Context, _ hash.Type) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

func TestCopyTransferTimeoutVerification(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferTimeout = fs.Duration(50 * time.Millisecond)
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
	require.NoError(t, err)
	dst.Features().Copy = nil
	srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	srcFs.(*mockfs.Fs).SetHashes(hash.NewHashSet(hash.MD5))
	src := mockobject.New("file").WithContent([]byte("data"), mockobject.SeekModeNone)
	src.SetFs(srcFs)
	_, err = operations.Copy(ctx, dst, nil, "file", timeoutHashObject{src})
	require.ErrorIs(t, err, fs.ErrorTransferTimeout)
	_, err = dst.NewObject(ctx, "file")
	require.NoError(t, err, "校验取消不能误删已上传对象")
	assert.EqualValues(t, 1, accounting.Stats(ctx).GetErrors())
	assert.Zero(t, accounting.Stats(ctx).GetTransfers())
}

func TestCopyTransferTimeoutUnknownSize(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferTimeout = fs.Duration(time.Second)
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
	require.NoError(t, err)
	dst.Features().Copy = nil
	srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	src := mockobject.New("file").WithContent([]byte("data"), mockobject.SeekModeNone)
	src.SetFs(srcFs)
	src.SetUnknownSize(true)
	_, err = operations.Copy(ctx, dst, nil, "file", src)
	require.NoError(t, err)
	assert.Zero(t, accounting.Stats(ctx).GetErrors())
	assert.EqualValues(t, 1, accounting.Stats(ctx).GetTransfers(), "rcat 和 Copy 必须共用同一次传输统计")
}

func TestTransferTimeoutRetryAfter(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferTimeout = fs.Duration(30 * time.Millisecond)
	ctx, cancel := fs.WithTransferTimeout(ctx)
	defer cancel()
	calls := 0
	err := operations.Retry(ctx, "file", 100, func() error {
		calls++
		return pacer.RetryAfterError(errors.New("rate limited"), time.Hour)
	})
	assert.ErrorIs(t, err, fs.ErrorTransferTimeout)
	assert.Equal(t, 1, calls)
}

func (o slowOpenObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	timer := time.NewTimer(o.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return o.Object.Open(ctx, options...)
	}
}

func TestCopyTransferTimeout(t *testing.T) {
	for _, timeout := range []string{"50ms", "0", "off"} {
		t.Run(timeout, func(t *testing.T) {
			ctx, ci := fs.AddConfig(context.Background())
			require.NoError(t, configstruct.Set(configmap.Simple{"transfer_timeout": timeout}, ci))
			ctx = accounting.WithStatsGroup(ctx, t.Name())
			dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
			require.NoError(t, err)
			dst.Features().Copy = nil
			srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
			require.NoError(t, err)
			src := mockobject.New("slow").WithContent([]byte("contents"), mockobject.SeekModeNone)
			src.SetFs(srcFs)
			_, err = operations.Copy(ctx, dst, nil, "slow", slowOpenObject{Object: src, delay: 200 * time.Millisecond})
			if timeout == "50ms" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "transfer timeout")
				assert.EqualValues(t, 1, accounting.Stats(ctx).GetErrors())
				assert.Zero(t, accounting.Stats(ctx).GetTransfers())
			} else {
				require.NoError(t, err)
				assert.Zero(t, accounting.Stats(ctx).GetErrors())
			}
			// 下一个对象必须使用新的预算，且父 context 仍可用。
			require.NoError(t, ctx.Err())
			_, err = operations.Copy(ctx, dst, nil, "good", src)
			require.NoError(t, err)
		})
	}
}
