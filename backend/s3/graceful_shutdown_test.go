package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGracefulShutdownMultipart(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx, ci := fs.AddConfig(context.Background())
			ci.TransferTimeout = fs.Duration(3 * time.Second)
			ci.LowLevelRetries = 1
			ci.MultiThreadStreams = 0
			ci.BufferSize = 0
			ctx = accounting.WithStatsGroup(ctx, t.Name())
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			ctx, shutdown := fs.WithGracefulShutdown(ctx)
			var parts, aborts, completes atomic.Int32
			var once sync.Once
			started, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
					_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>file</Key><UploadId>upload</UploadId></InitiateMultipartUploadResult>`)
				case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") == "upload":
					parts.Add(1)
					h := md5.New()
					_, _ = io.Copy(h, r.Body)
					once.Do(func() { close(started) })
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
					if fail {
						w.WriteHeader(http.StatusBadRequest)
						_, _ = io.WriteString(w, `<Error><Code>InvalidRequest</Code><Message>part failed</Message></Error>`)
						return
					}
					w.Header().Set("ETag", fmt.Sprintf(`"%x"`, h.Sum(nil)))
				case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") == "upload":
					completes.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					_, _ = io.WriteString(w, `<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>file</Key><ETag>"0123456789abcdef0123456789abcdef-3"</ETag></CompleteMultipartUploadResult>`)
				case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") == "upload":
					aborts.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			dst := timeoutTestFs(t, ctx, server.URL, true)
			dst.opt.UploadCutoff = 5 * 1024 * 1024
			srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
			require.NoError(t, err)
			src := mockobject.New("file").WithContent(bytes.Repeat([]byte("x"), 11*1024*1024), mockobject.SeekModeNone)
			src.SetFs(srcFs)
			done := make(chan error, 1)
			go func() {
				_, err := operations.Copy(ctx, dst, nil, "file", src)
				done <- err
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("multipart did not start")
			}
			shutdown.Stop()
			select {
			case err := <-done:
				t.Fatalf("multipart returned before release: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			assert.Zero(t, aborts.Load(), "graceful stop must not abort active multipart uploads")
			close(release)
			err = <-done
			if fail {
				require.Error(t, err)
				assert.EqualValues(t, 1, aborts.Load())
				assert.Zero(t, completes.Load())
			} else {
				require.NoError(t, err)
				assert.Zero(t, aborts.Load())
				assert.EqualValues(t, 1, completes.Load())
				assert.EqualValues(t, 3, parts.Load())
			}
		})
	}
}

func TestGracefulShutdownHTTPTimeout(t *testing.T) {
	for _, idle := range []bool{false, true} {
		t.Run(fmt.Sprint(idle), func(t *testing.T) {
			ctx, ci := fs.AddConfig(context.Background())
			ci.Timeout = fs.Duration(500 * time.Millisecond)
			ci.TransferTimeout = 0
			ci.LowLevelRetries = 1
			ci.MultiThreadStreams = 0
			ci.BufferSize = 0
			ctx = accounting.WithStatsGroup(ctx, t.Name())
			ctx, shutdown := fs.WithGracefulShutdown(ctx)
			const chunkSize = 64 * 1024
			data := bytes.Repeat([]byte("x"), 16*chunkSize)
			etag := fmt.Sprintf(`"%x"`, md5.Sum(data))
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", strconv.Itoa(len(data)))
				w.Header().Set("ETag", etag)
				w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
				shutdown.Stop()
				for off := 0; off < len(data); off += chunkSize {
					if _, err := w.Write(data[off : off+chunkSize]); err != nil {
						return
					}
					w.(http.Flusher).Flush()
					if idle {
						<-r.Context().Done()
						return
					}
					select {
					case <-r.Context().Done():
						return
					case <-time.After(50 * time.Millisecond):
					}
				}
			}))
			defer source.Close()
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("ETag", etag)
			}))
			defer target.Close()
			srcFs, dst := timeoutTestFs(t, ctx, source.URL, true), timeoutTestFs(t, ctx, target.URL, true)
			src, err := srcFs.NewObject(ctx, "file")
			require.NoError(t, err)
			src.(*Object).bytes = int64(len(data))
			start := time.Now()
			_, err = operations.Copy(ctx, dst, nil, "file", src)
			if idle {
				require.Error(t, err)
				assert.Less(t, time.Since(start), 2*time.Second)
			} else {
				require.NoError(t, err)
				assert.Greater(t, time.Since(start), time.Duration(ci.Timeout))
				assert.EqualValues(t, 1, accounting.Stats(ctx).GetTransfers())
			}
		})
	}
}
