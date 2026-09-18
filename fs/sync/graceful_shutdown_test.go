package sync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	mutex "sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type gracefulObject struct {
	*mockobject.ContentMockObject
	started chan<- context.Context
	release <-chan struct{}
}

type gracefulModTimeObject struct {
	fs.Object
	updates  *int
	removes  *int
	onRemove func()
	inPlace  bool
}

func (o gracefulModTimeObject) SetModTime(ctx context.Context, modTime time.Time) error {
	*o.updates++
	if o.inPlace {
		o.onRemove()
		return o.Object.SetModTime(ctx, modTime)
	}
	return fs.ErrorCantSetModTimeWithoutDelete
}

func (o gracefulModTimeObject) Remove(ctx context.Context) error {
	*o.removes++
	o.onRemove()
	return o.Object.Remove(ctx)
}

func TestGracefulShutdownDeferredChanges(t *testing.T) {
	for _, change := range []string{"modtime", "modtime-only", "rename", "rename-only"} {
		for _, stage := range []string{"queued", "admitted"} {
			t.Run(change+"/"+stage, func(t *testing.T) {
				ctx, ci := fs.AddConfig(context.Background())
				ci.NameTransform = nil
				ci.FixCase = change == "rename" || change == "rename-only"
				ci.BackupDir = ""
				ci.CheckSum = false
				ci.NoUpdateModTime = false
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				ctx = accounting.WithStatsGroup(ctx, t.Name())
				ctx, shutdown := fs.WithGracefulShutdown(ctx)
				srcFs, err := fs.NewFs(ctx, ":memory:"+t.Name()+"-src")
				require.NoError(t, err)
				dstFs, err := fs.NewFs(ctx, ":memory:"+t.Name()+"-dst")
				require.NoError(t, err)
				sourceName, targetName := "file", "file"
				before, after := "old data", "old data"
				if ci.FixCase {
					sourceName = "FILE"
					if change == "rename" {
						after = "new data with a different size"
					}
				}
				put := func(f fs.Fs, remote, data string, modtime time.Time) fs.Object {
					o, err := f.Put(ctx, bytes.NewBufferString(data), object.NewStaticObjectInfo(remote, modtime, int64(len(data)), true, nil, f))
					require.NoError(t, err)
					return o
				}
				src := put(srcFs, sourceName, after, t1)
				dst := put(dstFs, targetName, before, t1)
				updates, removes, moves := 0, 0, 0
				onChange := func() {
					if stage == "admitted" {
						shutdown.Stop()
					}
				}
				if !ci.FixCase {
					require.NoError(t, dst.SetModTime(ctx, t2))
					dst = gracefulModTimeObject{dst, &updates, &removes, onChange, change == "modtime-only"}
				} else {
					copyFn := dstFs.Features().Copy
					dstFs.Features().Move = func(ctx context.Context, o fs.Object, remote string) (fs.Object, error) {
						moves++
						onChange()
						moved, err := copyFn(ctx, o, remote)
						if err == nil {
							err = o.Remove(ctx)
						}
						return moved, err
					}
				}
				s, err := newSyncCopyMove(ctx, dstFs, srcFs, fs.DeleteModeOff, false, false, false, false)
				require.NoError(t, err)
				defer s.cancel()
				defer s.inCancel()
				s.inCtx, cancel = fs.WithGracefulInput(s.inCtx)
				defer cancel()
				in, err := newPipe("", func(int, int64) {}, 1)
				require.NoError(t, err)
				out, err := newPipe("", func(items int, _ int64) {
					if stage == "queued" && items > 0 {
						shutdown.Stop()
					}
				}, 1)
				require.NoError(t, err)
				require.True(t, in.Put(ctx, fs.ObjectPair{Src: src, Dst: dst}))
				in.Close()
				var wg mutex.WaitGroup
				wg.Add(1)
				s.pairChecker(in, out, 0, &wg)
				assert.Zero(t, updates, "checking must not update the destination")
				assert.Zero(t, removes, "checking must not delete the destination")
				assert.Zero(t, moves, "checking must not rename the destination")
				out.Close()
				wg.Add(1)
				s.pairCopyOrMove(s.ctx, out, dstFs, 0, &wg)
				require.NoError(t, s.currentError())
				wantName, wantData := targetName, before
				if stage == "admitted" {
					wantName, wantData = sourceName, after
				}
				o, err := dstFs.NewObject(ctx, wantName)
				require.NoError(t, err)
				reader, err := o.Open(ctx)
				require.NoError(t, err)
				data, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
				assert.Equal(t, wantData, string(data))
				if stage == "admitted" && change == "modtime-only" {
					assert.Equal(t, t1, o.ModTime(ctx))
					assert.Zero(t, accounting.Stats(ctx).GetTransfers(), "updating the time must not copy the object")
				}
			})
		}
	}
}

func TestGracefulShutdownDequeued(t *testing.T) {
	ctx, shutdown := fs.WithGracefulShutdown(context.Background())
	ctx, ci := fs.AddConfig(ctx)
	ci.NameTransform = nil
	src, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
	require.NoError(t, err)
	s, err := newSyncCopyMove(ctx, dst, src, fs.DeleteModeOff, false, false, false, false)
	require.NoError(t, err)
	defer s.cancel()
	defer s.inCancel()
	dequeued := false
	p, err := newPipe("", func(items int, _ int64) {
		if items == 0 {
			dequeued = true
			shutdown.Stop()
		}
	}, 1)
	require.NoError(t, err)
	started := make(chan context.Context, 1)
	o := gracefulObject{mockobject.New("file").WithContent([]byte("data"), mockobject.SeekModeNone), started, nil}
	require.True(t, p.Put(ctx, fs.ObjectPair{Src: o}))
	p.Close()
	var wg mutex.WaitGroup
	wg.Add(1)
	s.pairCopyOrMove(ctx, p, dst, 0, &wg)
	assert.True(t, dequeued)
	assert.Empty(t, started, "stop between dequeue and admission must skip the object")
}

func TestGracefulShutdownIdleWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx, ci := fs.AddConfig(ctx)
	ci.NameTransform = nil
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	ctx, shutdown := fs.WithGracefulShutdown(ctx)
	src, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
	require.NoError(t, err)
	s, err := newSyncCopyMove(ctx, dst, src, fs.DeleteModeOff, false, false, false, false)
	require.NoError(t, err)
	defer s.cancel()
	defer s.inCancel()
	s.inCtx, cancel = fs.WithGracefulInput(s.inCtx)
	defer cancel()
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	o := gracefulObject{mockobject.New("file").WithContent([]byte("data"), mockobject.SeekModeNone), started, release}
	o.SetFs(src)
	require.True(t, s.toBeUploaded.Put(ctx, fs.ObjectPair{Src: o}))
	finished := make(chan struct{}, 8)
	var wg mutex.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			s.pairCopyOrMove(ctx, s.toBeUploaded, dst, 0, &wg)
			finished <- struct{}{}
		}()
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("worker did not start")
	}
	shutdown.Stop()
	for range 7 {
		select {
		case <-finished:
		case <-ctx.Done():
			t.Fatal("idle worker did not exit")
		}
	}
	assert.Empty(t, finished, "active worker must finish its object first")
	close(release)
	wg.Wait()
	assert.NoError(t, s.currentError())
	assert.EqualValues(t, 1, accounting.Stats(ctx).GetTransfers())
}

type gracefulListFs struct {
	fs.Fs
	started  chan struct{}
	finished chan struct{}
}

type gracefulLookupFs struct {
	fs.Fs
	started chan struct{}
}

func (f gracefulLookupFs) NewObject(ctx context.Context, _ string) (fs.Object, error) {
	close(f.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestGracefulShutdownCopyDestLookup(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.NameTransform = nil
	ci.CopyDest = nil
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	ctx, shutdown := fs.WithGracefulShutdown(ctx)
	src, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	src.(*mockfs.Fs).AddObject(mockobject.New("file").WithContent([]byte("data"), mockobject.SeekModeNone))
	dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
	require.NoError(t, err)
	s, err := newSyncCopyMove(ctx, dst, src, fs.DeleteModeOff, false, false, false, false)
	require.NoError(t, err)
	lookup := gracefulLookupFs{Fs: dst, started: make(chan struct{})}
	ci.CopyDest = []string{"copy-dest"}
	s.compareCopyDest = []fs.Fs{lookup}
	done := make(chan error, 1)
	go func() { done <- s.run() }()
	select {
	case <-lookup.started:
	case <-ctx.Done():
		t.Fatal("copy-dest lookup did not start")
	}
	shutdown.Stop()
	require.NoError(t, <-done)
	require.NoError(t, ctx.Err())
	assert.Zero(t, accounting.Stats(ctx).GetErrors())
}

func (f gracefulListFs) List(ctx context.Context, _ string) (fs.DirEntries, error) {
	close(f.started)
	<-ctx.Done()
	close(f.finished)
	return nil, fmt.Errorf("listing stopped: %w", ctx.Err())
}

func TestGracefulShutdownListing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	ctx, shutdown := fs.WithGracefulShutdown(ctx)
	src, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	source := gracefulListFs{src, make(chan struct{}), make(chan struct{})}
	dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- CopyDir(ctx, dst, source, false) }()
	select {
	case <-source.started:
	case <-ctx.Done():
		t.Fatal("listing did not start")
	}
	shutdown.Stop()
	require.NoError(t, <-done)
	assert.Zero(t, accounting.Stats(ctx).GetErrors())
	select {
	case <-source.finished:
	default:
		t.Fatal("returned with listing still running")
	}
}

func TestGracefulShutdownChecking(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.CheckFirst = true
	ci.NameTransform = nil
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	ctx, shutdown := fs.WithGracefulShutdown(ctx)
	src, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	dst, err := mockfs.NewFs(ctx, "target", "", nil)
	require.NoError(t, err)
	for _, f := range []fs.Fs{src, dst} {
		f.(*mockfs.Fs).AddObject(mockobject.New("file").WithContent([]byte("data"), mockobject.SeekModeNone))
	}
	started := make(chan struct{})
	ctx = operations.WithEqualFn(ctx, func(ctx context.Context, _ fs.ObjectInfo, _ fs.Object) bool {
		close(started)
		<-ctx.Done()
		return false
	})
	done := make(chan error, 1)
	go func() { done <- CopyDir(ctx, dst, src, false) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("checking did not start")
	}
	shutdown.Stop()
	require.NoError(t, <-done)
	assert.Zero(t, accounting.Stats(ctx).GetErrors())
	assert.Zero(t, accounting.Stats(ctx).GetTransfers())
}

func TestGracefulShutdownDeadlines(t *testing.T) {
	for _, mode := range []string{"transfer", "hard", "soft", "off", "zero"} {
		t.Run(mode, func(t *testing.T) {
			ctx, ci := fs.AddConfig(context.Background())
			ci.NameTransform = nil
			ci.MaxDuration = 0
			ci.TransferTimeout = 0
			ci.CutoffMode = fs.CutoffModeSoft
			switch mode {
			case "transfer":
				ci.TransferTimeout = fs.Duration(150 * time.Millisecond)
			case "hard":
				ci.CutoffMode = fs.CutoffModeHard
				fallthrough
			case "soft":
				ci.MaxDuration = fs.Duration(150 * time.Millisecond)
			case "off":
				ci.TransferTimeout = fs.DurationOff
			}
			ctx = accounting.WithStatsGroup(ctx, t.Name())
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			ctx, shutdown := fs.WithGracefulShutdown(ctx)
			src, err := mockfs.NewFs(ctx, "source", "", nil)
			require.NoError(t, err)
			dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
			require.NoError(t, err)
			started := make(chan context.Context, 1)
			release := make(chan struct{})
			src.(*mockfs.Fs).AddObject(gracefulObject{mockobject.New("file").WithContent([]byte("data"), mockobject.SeekModeNone), started, release})
			done := make(chan error, 1)
			go func() { done <- CopyDir(ctx, dst, src, false) }()
			var active context.Context
			select {
			case active = <-started:
			case <-ctx.Done():
				t.Fatal("transfer did not start")
			}
			before, _ := active.Deadline()
			shutdown.Stop()
			after, _ := active.Deadline()
			assert.Equal(t, before, after)
			if mode == "hard" || mode == "transfer" {
				err = <-done
				if mode == "transfer" {
					assert.ErrorIs(t, err, fs.ErrorTransferTimeout)
				} else {
					assert.ErrorIs(t, err, ErrorMaxDurationReached)
				}
				assert.Zero(t, accounting.Stats(ctx).GetTransfers())
			} else {
				time.Sleep(200 * time.Millisecond)
				assert.NoError(t, active.Err())
				close(release)
				err = <-done
				if mode == "soft" {
					assert.ErrorIs(t, err, ErrorMaxDurationReached)
				} else {
					assert.NoError(t, err)
				}
				assert.EqualValues(t, 1, accounting.Stats(ctx).GetTransfers())
			}
		})
	}
}

func (o gracefulObject) Open(ctx context.Context, opts ...fs.OpenOption) (io.ReadCloser, error) {
	o.started <- ctx
	select {
	case <-o.release:
		return o.ContentMockObject.Open(ctx, opts...)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestGracefulShutdownQueue(t *testing.T) {
	for _, workers := range []int{1, 8} {
		for _, mode := range []string{"normal", "no-traverse", "check-first"} {
			t.Run(fmt.Sprintf("%d/%s", workers, mode), func(t *testing.T) {
				ctx, ci := fs.AddConfig(context.Background())
				ci.NameTransform = nil
				ci.Transfers, ci.Checkers, ci.MaxBacklog = workers, 8, 2
				ci.CheckFirst = mode == "check-first"
				ci.NoTraverse = mode == "no-traverse"
				ctx = accounting.WithStatsGroup(ctx, t.Name())
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				ctx, shutdown := fs.WithGracefulShutdown(ctx)
				srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
				require.NoError(t, err)
				source := srcFs.(*mockfs.Fs)
				dst, err := fs.NewFs(ctx, ":memory:"+t.Name())
				require.NoError(t, err)
				dst.Features().Copy = nil
				started := make(chan context.Context, workers+256)
				release := make(chan struct{})
				for i := range workers + 256 {
					source.AddObject(gracefulObject{
						mockobject.New(fmt.Sprintf("file-%02d", i)).WithContent([]byte("data"), mockobject.SeekModeNone), started, release,
					})
				}
				done := make(chan error, 1)
				go func() { done <- CopyDir(ctx, dst, source, false) }()
				for range workers {
					select {
					case <-started:
					case <-ctx.Done():
						t.Fatal("workers did not start")
					}
				}
				shutdown.Stop()
				select {
				case err := <-done:
					t.Fatalf("returned before draining: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				close(release)
				require.NoError(t, <-done)
				assert.Empty(t, started, "queued objects must not start")
				assert.EqualValues(t, workers, accounting.Stats(ctx).GetTransfers())
				assert.Zero(t, accounting.Stats(ctx).GetErrors())
			})
		}
	}
}
