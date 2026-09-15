package sync

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stalledTransferObject struct {
	*mockobject.ContentMockObject
	active *atomic.Int32
	peak   *atomic.Int32
}

func (o stalledTransferObject) Open(ctx context.Context, _ ...fs.OpenOption) (io.ReadCloser, error) {
	n := o.active.Add(1)
	defer o.active.Add(-1)
	for old := o.peak.Load(); n > old; old = o.peak.Load() {
		if o.peak.CompareAndSwap(old, n) {
			break
		}
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestTransferTimeoutContinuesQueue(t *testing.T) {
	for _, workers := range []int{1, 8} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			ctx, ci := fs.AddConfig(context.Background())
			ci.NameTransform = nil
			ci.TransferTimeout = fs.Duration(200 * time.Millisecond)
			ci.Transfers, ci.Checkers, ci.MaxBacklog = workers, 8, 2
			ci.IgnoreExisting = true
			ci.NoTraverse = true
			ctx = accounting.WithStatsGroup(ctx, t.Name())
			// 整批保护时间仅防止测试永久挂起；不能代替每对象时限。
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
			require.NoError(t, err)
			source := srcFs.(*mockfs.Fs)
			dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
			require.NoError(t, err)
			dst.Features().Copy = nil
			var active, peak atomic.Int32
			for i := range workers {
				o := mockobject.New(fmt.Sprintf("a-bad-%02d", i)).WithContent([]byte("bad"), mockobject.SeekModeNone)
				source.AddObject(stalledTransferObject{o, &active, &peak})
			}
			for i := range 32 {
				source.AddObject(mockobject.New(fmt.Sprintf("b-good-%02d", i)).WithContent([]byte("good"), mockobject.SeekModeNone))
			}
			err = CopyDir(ctx, dst, source, false)
			require.ErrorIs(t, err, fs.ErrorTransferTimeout)
			require.NoError(t, ctx.Err(), "必须在整批 context 到期前完成剩余对象")
			assert.EqualValues(t, workers, accounting.Stats(ctx).GetErrors())
			assert.EqualValues(t, 32, accounting.Stats(ctx).GetTransfers())
			assert.True(t, accounting.Stats(ctx).HadTransferTimeout())
			assert.Zero(t, active.Load())
			assert.EqualValues(t, workers, peak.Load())
			for i := range 32 {
				o, err := dst.NewObject(ctx, fmt.Sprintf("b-good-%02d", i))
				require.NoError(t, err)
				assert.EqualValues(t, 4, o.Size())
			}
		})
	}
}
