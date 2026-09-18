package operations_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type gracefulRetryFs struct {
	fs.Fs
	shutdown *fs.GracefulShutdown
	calls    int
	deadline time.Time
}

func (f *gracefulRetryFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	f.calls++
	if f.calls == 1 {
		f.deadline, _ = ctx.Deadline()
		f.shutdown.Stop()
		return nil, fserrors.RetryError(errors.New("retry the active object"))
	}
	deadline, _ := ctx.Deadline()
	if deadline != f.deadline || ctx.Err() != nil {
		return nil, errors.New("active object context changed")
	}
	return f.Fs.Put(ctx, in, src, options...)
}

func TestGracefulShutdownCopyRetries(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferTimeout = fs.Duration(time.Second)
	ci.LowLevelRetries = 3
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	ctx, shutdown := fs.WithGracefulShutdown(ctx)
	dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
	require.NoError(t, err)
	dst.Features().Copy = nil
	srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	src := mockobject.New("file").WithContent([]byte("data"), mockobject.SeekModeNone)
	src.SetFs(srcFs)
	target := &gracefulRetryFs{Fs: dst, shutdown: shutdown}
	_, err = operations.Copy(ctx, target, nil, "file", src)
	require.NoError(t, err)
	assert.Equal(t, 2, target.calls)
	assert.EqualValues(t, 1, accounting.Stats(ctx).GetTransfers())
	assert.Zero(t, accounting.Stats(ctx).GetErrors())
	_, err = operations.Copy(ctx, target, nil, "skipped", src)
	require.NoError(t, err)
	assert.Equal(t, 2, target.calls)
	require.NoError(t, operations.CopyFile(ctx, nil, nil, "skipped", "skipped"))
}

func TestGracefulShutdownCopyDest(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failure], func(t *testing.T) {
			ctx, ci := fs.AddConfig(context.Background())
			ci.CopyDest = []string{"copy-dest"}
			ci.SizeOnly = true
			ctx = accounting.WithStatsGroup(ctx, t.Name())
			ctx, shutdown := fs.WithGracefulShutdown(ctx)
			src, err := fs.NewFs(ctx, ":memory:"+t.Name()+"-source")
			require.NoError(t, err)
			dst, err := fs.NewFs(ctx, ":memory:"+t.Name()+"-target")
			require.NoError(t, err)
			o, err := src.Put(ctx, bytes.NewReader([]byte("data")), object.NewStaticObjectInfo("file", time.Now(), 4, true, nil, src))
			require.NoError(t, err)
			copyFn := dst.Features().Copy
			calls := 0
			dst.Features().Copy = func(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
				calls++
				shutdown.Stop()
				assert.NoError(t, ctx.Err())
				if failure {
					return nil, errors.New("real copy-dest failure")
				}
				return copyFn(ctx, src, remote)
			}
			input, cancel := fs.WithGracefulInput(ctx)
			defer cancel()
			skipped, err := operations.CompareOrCopyDest(input, dst, nil, o, []fs.Fs{src}, nil)
			require.True(t, skipped)
			if failure {
				require.ErrorContains(t, err, "real copy-dest failure")
				assert.EqualValues(t, 1, accounting.Stats(ctx).GetErrors())
			} else {
				require.NoError(t, err)
				assert.EqualValues(t, 1, accounting.Stats(ctx).GetTransfers())
			}
			_, err = operations.CompareOrCopyDest(ctx, dst, nil, o, []fs.Fs{src}, nil)
			require.NoError(t, err)
			assert.Equal(t, 1, calls)
		})
	}
}

type gracefulCopyDestObject struct {
	fs.Object
	shutdown *fs.GracefulShutdown
}

func (o gracefulCopyDestObject) SetModTime(ctx context.Context, _ time.Time) error {
	o.shutdown.Stop()
	return fs.ErrorCantSetModTimeWithoutDelete
}

func TestGracefulShutdownCopyDestModTime(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.CopyDest = []string{"copy-dest"}
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	ctx, shutdown := fs.WithGracefulShutdown(ctx)
	src, err := fs.NewFs(ctx, ":memory:"+t.Name()+"-source")
	require.NoError(t, err)
	dst, err := fs.NewFs(ctx, ":memory:"+t.Name()+"-target")
	require.NoError(t, err)
	modTime := time.Now().Add(-time.Hour)
	srcObj, err := src.Put(ctx, bytes.NewBufferString("data"), object.NewStaticObjectInfo("file", modTime, 4, true, nil, src))
	require.NoError(t, err)
	dstObj, err := dst.Put(ctx, bytes.NewBufferString("data"), object.NewStaticObjectInfo("file", modTime.Add(time.Hour), 4, true, nil, dst))
	require.NoError(t, err)
	input, cancel := fs.WithGracefulInput(ctx)
	defer cancel()
	skipped, err := operations.CompareOrCopyDest(input, dst, gracefulCopyDestObject{dstObj, shutdown}, srcObj, []fs.Fs{src}, nil)
	require.True(t, skipped)
	require.NoError(t, err)
	require.True(t, shutdown.Stopped())
	result, err := dst.NewObject(ctx, "file")
	require.NoError(t, err, "the admitted copy must replace the deleted destination")
	assert.Equal(t, modTime, result.ModTime(ctx))
	assert.EqualValues(t, 1, accounting.Stats(ctx).GetTransfers())
	assert.Zero(t, accounting.Stats(ctx).GetErrors())
}
