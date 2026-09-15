package multipart

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type timeoutChunkWriter struct {
	t          *testing.T
	started    chan struct{}
	active     atomic.Bool
	aborted    bool
	leaveParts bool
}

func (w *timeoutChunkWriter) OpenChunkWriter(context.Context, string, fs.ObjectInfo, ...fs.OpenOption) (fs.ChunkWriterInfo, fs.ChunkWriter, error) {
	return fs.ChunkWriterInfo{ChunkSize: 4096, Concurrency: 2, LeavePartsOnError: w.leaveParts}, w, nil
}

func (w *timeoutChunkWriter) WriteChunk(ctx context.Context, _ int, in io.ReadSeeker) (int64, error) {
	w.active.Store(true)
	defer w.active.Store(false)
	close(w.started)
	<-ctx.Done()
	// 模拟 SDK 在收到取消后仍需收尾，验证 Abort 必须等待它结束。
	time.Sleep(30 * time.Millisecond)
	b := make([]byte, 1)
	_, err := in.Read(b)
	assert.NoError(w.t, err)
	assert.Equal(w.t, byte('x'), b[0])
	return 0, ctx.Err()
}

func (w *timeoutChunkWriter) Close(context.Context) error {
	w.t.Error("读取失败后不得提交 multipart")
	return nil
}

func (w *timeoutChunkWriter) Abort(ctx context.Context) error {
	assert.False(w.t, w.active.Load(), "Abort 前 WriteChunk 必须已经退出")
	assert.NoError(w.t, ctx.Err(), "Abort 不能使用已取消的上传 context")
	d, ok := ctx.Deadline()
	assert.True(w.t, ok)
	assert.LessOrEqual(w.t, time.Until(d), 30*time.Second)
	w.aborted = true
	return nil
}

type timeoutSource struct {
	ctx     context.Context
	started <-chan struct{}
}

func (r timeoutSource) Read([]byte) (int, error) {
	<-r.started
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func TestTransferTimeoutWaitsForChunks(t *testing.T) {
	for _, leaveParts := range []bool{false, true} {
		ctx, ci := fs.AddConfig(context.Background())
		ci.TransferTimeout = fs.Duration(30 * time.Millisecond)
		ctx, cancel := fs.WithTransferTimeout(ctx)
		w := &timeoutChunkWriter{t: t, started: make(chan struct{}), leaveParts: leaveParts}
		in := io.MultiReader(bytes.NewReader(bytes.Repeat([]byte("x"), 4096)), timeoutSource{ctx, w.started})
		src := object.NewStaticObjectInfo("bad", time.Now(), 8192, false, nil, nil)
		_, err := UploadMultipart(ctx, src, in, UploadMultipartOptions{Open: w})
		cancel()
		require.ErrorIs(t, err, fs.ErrorTransferTimeout)
		assert.False(t, w.active.Load(), "返回后不能遗留上传任务")
		assert.Equal(t, !leaveParts, w.aborted)
	}
}
