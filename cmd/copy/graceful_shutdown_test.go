//go:build !windows && !plan9

package copy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type signalTestObject struct {
	*mockobject.ContentMockObject
	release <-chan struct{}
	fail    bool
}

func (o signalTestObject) Open(ctx context.Context, opts ...fs.OpenOption) (io.ReadCloser, error) {
	fmt.Println("ACTIVE", o.Remote())
	select {
	case <-o.release:
		if o.fail {
			return nil, errors.New("test transfer failed")
		}
		return o.ContentMockObject.Open(ctx, opts...)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestGracefulShutdownCommand(t *testing.T) {
	const childEnv = "RCLONE_TEST_COPY_GRACEFUL"
	if mode := os.Getenv(childEnv); mode != "" {
		release := make(chan struct{})
		go func() {
			_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
			close(release)
		}()
		fs.Register(&fs.RegInfo{Name: "gracefultest", NewFs: func(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
			f, err := mockfs.NewFs(ctx, name, "", m)
			if err != nil {
				return nil, err
			}
			for i := range 64 {
				f.(*mockfs.Fs).AddObject(signalTestObject{mockobject.New(fmt.Sprintf("file-%02d", i)).WithContent([]byte("data"), mockobject.SeekModeNone), release, strings.Contains(mode, "failure")})
			}
			if root != "" {
				return f, fs.ErrorIsFile
			}
			return f, nil
		}})
		os.Args = append([]string{os.Args[0]}, strings.Split(os.Getenv("RCLONE_TEST_COPY_ARGS"), "\n")...)
		cmd.Main()
		return
	}
	for _, mode := range []string{"normal", "single", "check-first", "no-traverse", "files-from-raw", "failure", "single-failure"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			dst := filepath.Join(dir, "dst")
			report := filepath.Join(dir, "errors")
			source := ":gracefultest:"
			if strings.HasPrefix(mode, "single") {
				source += "file-00"
			}
			args := []string{"copy", source, dst, "--graceful-shutdown", "--transfers", "1", "--checkers", "1", "--max-backlog", "2", "--low-level-retries", "1", "--error", report, "--stats", "1h", "--stats-log-level", "NOTICE", "--config", filepath.Join(dir, "no-config")}
			if mode == "check-first" || mode == "no-traverse" {
				args = append(args, "--"+mode)
			}
			if mode == "files-from-raw" {
				files := filepath.Join(dir, "files")
				require.NoError(t, os.WriteFile(files, []byte("file-00\nfile-01\nfile-02\n"), 0600))
				args = append(args, "--files-from-raw", files)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGracefulShutdownCommand$")
			child.Env = append(os.Environ(), childEnv+"="+mode, "RCLONE_TEST_COPY_ARGS="+strings.Join(args, "\n"))
			out, err := child.StdoutPipe()
			require.NoError(t, err)
			child.Stderr = child.Stdout
			in, err := child.StdinPipe()
			require.NoError(t, err)
			require.NoError(t, child.Start())
			scanner := bufio.NewScanner(out)
			var output []string
			until := func(want string) {
				t.Helper()
				for scanner.Scan() {
					line := scanner.Text()
					output = append(output, line)
					if strings.Contains(line, want) {
						return
					}
				}
				t.Fatalf("missing %q in %s", want, strings.Join(output, "\n"))
			}
			until("ACTIVE")
			require.NoError(t, child.Process.Signal(syscall.SIGTERM))
			until("waiting for active transfers")
			_, err = fmt.Fprintln(in, "finish")
			require.NoError(t, err)
			for scanner.Scan() {
				output = append(output, scanner.Text())
			}
			err = child.Wait()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, strings.Join(output, "\n"))
			assert.Equal(t, 143, exitErr.ExitCode(), strings.Join(output, "\n"))
			assert.Equal(t, 1, strings.Count(strings.Join(output, "\n"), "ACTIVE"))
			assert.Contains(t, strings.Join(output, "\n"), "Graceful shutdown complete")
			reportData, err := os.ReadFile(report)
			require.NoError(t, err)
			entries, err := os.ReadDir(dst)
			if strings.Contains(mode, "failure") {
				assert.Contains(t, string(reportData), "file-")
				assert.Equal(t, 1, strings.Count(string(reportData), "\n"))
				assert.Empty(t, entries)
			} else {
				require.NoError(t, err)
				assert.Empty(t, reportData)
				require.Len(t, entries, 1)
				data, err := os.ReadFile(filepath.Join(dst, entries[0].Name()))
				require.NoError(t, err)
				assert.Equal(t, "data", string(data))
			}
		})
	}
}
