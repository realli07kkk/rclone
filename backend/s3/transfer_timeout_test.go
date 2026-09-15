package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func timeoutTestFs(t *testing.T, ctx context.Context, endpoint string, noHead bool) *Fs {
	t.Helper()
	name := fmt.Sprintf(":s3,provider=Other,access_key_id=probe,secret_access_key=probe,region=us-east-1,endpoint='%s',no_check_bucket=true,no_head_object=true,no_head=%t:bucket", endpoint, noHead)
	f, err := fs.NewFs(ctx, name)
	require.NoError(t, err)
	f.Features().Copy = nil
	return f.(*Fs)
}

func TestTransferTimeoutHTTP(t *testing.T) {
	for _, variant := range []struct {
		buffered bool
		http2    bool
		bwlimit  fs.SizeSuffix
	}{{}, {buffered: true}, {buffered: true, http2: true}, {buffered: true, bwlimit: 10 * 1024 * 1024}, {buffered: true, http2: true, bwlimit: 30 * 1024 * 1024}} {
		buffered := variant.buffered
		for _, mode := range []string{"headers", "partial", "flow", "target-response", "target-write", "verify", "normal"} {
			t.Run(fmt.Sprintf("%s/buffer=%t/http2=%t/bw=%s", mode, buffered, variant.http2, variant.bwlimit), func(t *testing.T) {
				ctx, ci := fs.AddConfig(context.Background())
				ci.TransferTimeout = fs.Duration(150 * time.Millisecond)
				ci.InsecureSkipVerify = variant.http2 // 仅信任本机测试服务器的自签名证书。
				if variant.bwlimit > 0 {
					accounting.TokenBucket.SetBwLimit(fs.BwPair{Tx: variant.bwlimit, Rx: variant.bwlimit})
					defer accounting.TokenBucket.SetBwLimit(fs.BwPair{Tx: -1, Rx: -1})
					if mode == "normal" {
						ci.TransferTimeout = fs.Duration(time.Second)
					}
				}
				ci.Timeout = fs.Duration(2 * time.Second)
				ci.MultiThreadStreams = 0
				ci.BufferSize = 0
				if buffered {
					ci.BufferSize = 16 * 1024 * 1024
				}
				ctx = accounting.WithStatsGroup(ctx, t.Name())
				dataSize := 1024 * 1024
				if mode == "target-write" {
					dataSize = 32 * 1024 * 1024
				}
				data := bytes.Repeat([]byte("x"), dataSize)
				etag := fmt.Sprintf("\"%x\"", md5.Sum(data))
				var sourceHeads, sourceGets, targetPuts, targetDeletes, active atomic.Int32
				done := make(chan struct{})
				wait := func(r *http.Request) {
					select {
					case <-r.Context().Done():
					case <-done:
					}
				}
				newServer := func(h http.Handler) *httptest.Server {
					s := httptest.NewUnstartedServer(h)
					if variant.http2 {
						s.EnableHTTP2 = true
						s.StartTLS()
					} else {
						s.Start()
					}
					return s
				}
				source := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if variant.http2 {
						assert.Equal(t, 2, r.ProtoMajor)
					}
					active.Add(1)
					defer active.Add(-1)
					if r.Method == http.MethodHead {
						sourceHeads.Add(1)
						w.WriteHeader(500)
						return
					}
					sourceGets.Add(1)
					if mode == "headers" {
						wait(r)
						return
					}
					w.Header().Set("Content-Length", strconv.Itoa(len(data)))
					w.Header().Set("ETag", etag)
					w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
					w.WriteHeader(200)
					if mode == "partial" {
						_, _ = w.Write(data[:32*1024])
						w.(http.Flusher).Flush()
						wait(r)
						return
					}
					if mode == "flow" {
						for off := 0; off < len(data); off += 4096 {
							if _, err := w.Write(data[off : off+4096]); err != nil {
								return
							}
							w.(http.Flusher).Flush()
							select {
							case <-r.Context().Done():
								return
							case <-done:
								return
							case <-time.After(10 * time.Millisecond):
							}
						}
						return
					}
					_, _ = w.Write(data)
				}))
				defer source.Close()
				target := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if variant.http2 {
						assert.Equal(t, 2, r.ProtoMajor)
					}
					active.Add(1)
					defer active.Add(-1)
					if r.Method == http.MethodDelete {
						targetDeletes.Add(1)
						w.WriteHeader(204)
						return
					}
					if r.Method == http.MethodHead {
						wait(r)
						return
					}
					targetPuts.Add(1)
					if mode == "target-write" {
						// 不读请求体时服务端不一定立即感知断连，测试结束主动收尾。
						wait(r)
						return
					}
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						return
					}
					if mode == "target-response" {
						wait(r)
						return
					}
					w.Header().Set("ETag", etag)
					w.WriteHeader(200)
				}))
				defer target.Close()
				defer close(done)
				srcFs, dstFs := timeoutTestFs(t, ctx, source.URL, true), timeoutTestFs(t, ctx, target.URL, mode != "verify")
				if variant.http2 {
					// 每个测试 transport 独立协商 HTTP/2，不能复用 DefaultTransport 已绑定的回调。
					for _, f := range []*Fs{srcFs, dstFs} {
						f.srv.Transport.(*fshttp.Transport).TLSNextProto = nil
					}
				}
				src, err := srcFs.NewObject(ctx, "file")
				require.NoError(t, err)
				if buffered {
					src.(*Object).bytes = int64(len(data))
				}
				start := time.Now()
				_, err = operations.Copy(ctx, dstFs, nil, "file", src)
				if mode == "normal" {
					require.NoError(t, err)
					assert.EqualValues(t, 1, accounting.Stats(ctx).GetTransfers())
				} else {
					require.ErrorIs(t, err, fs.ErrorTransferTimeout)
					assert.Less(t, time.Since(start), time.Second)
					assert.EqualValues(t, 1, accounting.Stats(ctx).GetErrors())
					assert.Zero(t, accounting.Stats(ctx).GetTransfers())
				}
				assert.Zero(t, sourceHeads.Load())
				assert.EqualValues(t, 1, sourceGets.Load(), "超时不得重开 GET")
				assert.Zero(t, targetDeletes.Load(), "超时不能删除最终对象")
				if mode != "headers" {
					assert.EqualValues(t, 1, targetPuts.Load())
				}
				if mode != "target-write" {
					assert.Eventually(t, func() bool { return active.Load() == 0 }, time.Second, 5*time.Millisecond)
				}
			})
		}
	}
}

func TestTransferTimeoutAbortContext(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferTimeout = fs.Duration(10 * time.Millisecond)
	ctx, cancel := fs.WithTransferTimeout(ctx)
	defer cancel()
	<-ctx.Done()
	cleanupCtx, stop := fs.TransferCleanupContext(ctx)
	defer stop()
	cleanupCtx, stopRequest := context.WithTimeout(cleanupCtx, 60*time.Millisecond)
	defer stopRequest()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		assert.Equal(t, "upload", r.URL.Query().Get("uploadId"))
		<-r.Context().Done()
	}))
	defer server.Close()
	f := timeoutTestFs(t, context.Background(), server.URL, true)
	w := &s3ChunkWriter{f: f, bucket: aws.String("bucket"), key: aws.String("file"), uploadID: aws.String("upload")}
	// Abort 读取请求参数；使用真实 SDK 并验证它遵循传入的清理 deadline。
	w.multiPartUploadInput = &s3.CreateMultipartUploadInput{}
	start := time.Now()
	err := w.Abort(cleanupCtx)
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
	assert.EqualValues(t, 1, requests.Load())
}

func TestTransferTimeoutMultipartHTTP(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferTimeout = fs.Duration(200 * time.Millisecond)
	ci.Timeout = fs.Duration(2 * time.Second)
	ci.MultiThreadStreams = 0
	ci.BufferSize = 0
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	var parts, aborts, heads, active atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			heads.Add(1)
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Length", "13463630")
		w.Header().Set("ETag", `"0123456789abcdef0123456789abcdef"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		_, _ = w.Write(bytes.Repeat([]byte("x"), 8_388_279))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer source.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>file</Key><UploadId>upload</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") == "upload":
			parts.Add(1)
			active.Add(1)
			defer active.Add(-1)
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
		case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") == "upload":
			aborts.Add(1)
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(500)
		}
	}))
	defer target.Close()
	srcFs, dstFs := timeoutTestFs(t, ctx, source.URL, true), timeoutTestFs(t, ctx, target.URL, true)
	dstFs.opt.UploadCutoff = 5 * 1024 * 1024
	src, err := srcFs.NewObject(ctx, "file")
	require.NoError(t, err)
	_, err = operations.Copy(ctx, dstFs, nil, "file", src)
	require.ErrorIs(t, err, fs.ErrorTransferTimeout)
	assert.GreaterOrEqual(t, parts.Load(), int32(1))
	assert.EqualValues(t, 1, aborts.Load())
	assert.Zero(t, heads.Load())
	assert.Eventually(t, func() bool { return active.Load() == 0 }, time.Second, 5*time.Millisecond)
	assert.EqualValues(t, 1, accounting.Stats(ctx).GetErrors())
}

func TestTransferTimeoutResourceReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("重复故障资源检查")
	}
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferTimeout = fs.Duration(30 * time.Millisecond)
	ci.Timeout = fs.Duration(2 * time.Second)
	ci.MultiThreadStreams = 0
	ci.BufferSize = 0
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	var active, connections atomic.Int32
	// 并发传输共用一个预先建立的统计组，与 sync 启动流程一致。
	stats := accounting.Stats(ctx)
	data := bytes.Repeat([]byte("x"), 1024)
	etag := fmt.Sprintf("\"%x\"", md5.Sum(data))
	connState := func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		} else if state == http.StateClosed {
			connections.Add(-1)
		}
	}
	source := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active.Add(1)
		defer active.Add(-1)
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		if strings.Contains(r.URL.Path, "/bad-") {
			_, _ = w.Write(data[:512])
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		_, _ = w.Write(data)
	}))
	source.Config.ConnState = connState
	source.Start()
	defer source.Close()
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active.Add(1)
		defer active.Add(-1)
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return
		}
		w.Header().Set("ETag", etag)
		w.WriteHeader(200)
	}))
	target.Config.ConnState = connState
	target.Start()
	defer target.Close()
	srcFs, dstFs := timeoutTestFs(t, ctx, source.URL, true), timeoutTestFs(t, ctx, target.URL, true)
	batch := func() {
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() {
				src, err := srcFs.NewObject(ctx, fmt.Sprintf("bad-%d", i))
				if !assert.NoError(t, err) {
					return
				}
				_, err = operations.Copy(ctx, dstFs, nil, src.Remote(), src)
				assert.ErrorIs(t, err, fs.ErrorTransferTimeout)
			})
		}
		wg.Wait()
	}
	idle := func() {
		assert.Eventually(t, func() bool { return active.Load() == 0 }, time.Second, 5*time.Millisecond)
		srcFs.srv.CloseIdleConnections()
		dstFs.srv.CloseIdleConnections()
		assert.Eventually(t, func() bool { return connections.Load() == 0 }, time.Second, 5*time.Millisecond)
	}
	batch()
	idle()
	baseline := runtime.NumGoroutine()
	for range 128 {
		batch()
	}
	idle()
	assert.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline+16 }, 2*time.Second, 10*time.Millisecond)
	assert.EqualValues(t, 8*129, stats.GetErrors())
	ci.TransferTimeout = fs.Duration(time.Second)
	src, err := srcFs.NewObject(ctx, "good")
	require.NoError(t, err)
	_, err = operations.Copy(ctx, dstFs, nil, "good", src)
	require.NoError(t, err, "连续超时后仍能完成正常对象")
}
