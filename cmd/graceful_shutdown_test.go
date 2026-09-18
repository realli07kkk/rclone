//go:build !windows && !plan9

package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/atexit"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGracefulShutdownSignals(t *testing.T) {
	const childEnv = "RCLONE_TEST_GRACEFUL_CHILD"
	if mode := os.Getenv(childEnv); mode != "" {
		command := &cobra.Command{Use: "copy"}
		if mode != "disabled" {
			defer EnableGracefulShutdown(command)()
		}
		ctx := command.Context()
		if ctx == nil {
			ctx = context.Background()
		}
		ci := fs.GetConfig(ctx)
		ci.Retries = 3
		ci.LogLevel = fs.LogLevelInfo
		if mode == "sleep" {
			ci.RetriesInterval = fs.Duration(time.Hour)
		}
		accounting.Start(ctx)
		atexit.Register(func() { fmt.Println("cleanup") })
		Run(true, false, command, func() error {
			fmt.Println("attempt")
			if mode == "sleep" {
				return errors.New("retryable failure")
			}
			if mode == "retry-after" {
				return fserrors.NewErrorRetryAfter(time.Hour)
			}
			fmt.Println("ready")
			if mode == "drain" || mode == "failure" {
				<-fs.GetGracefulShutdown(ctx).Done()
				fmt.Println("draining")
				_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
				fmt.Println("completed")
				if mode == "failure" {
					return errors.New("real transfer failure")
				}
				return nil
			}
			select {}
		})
		return
	}
	for _, mode := range []string{"drain", "failure", "second", "interrupt", "disabled", "sleep", "retry-after"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGracefulShutdownSignals$")
			child.Env = append(os.Environ(), childEnv+"="+mode)
			stdout, err := child.StdoutPipe()
			require.NoError(t, err)
			child.Stderr = child.Stdout
			stdin, err := child.StdinPipe()
			require.NoError(t, err)
			require.NoError(t, child.Start())
			lines := make(chan string, 100)
			go func() {
				defer close(lines)
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					lines <- scanner.Text()
				}
			}()
			var output []string
			until := func(want string) {
				t.Helper()
				for line := range lines {
					output = append(output, line)
					if strings.Contains(line, want) {
						return
					}
				}
				t.Fatalf("missing %q in %s", want, strings.Join(output, "\n"))
			}
			if mode == "sleep" {
				until("Attempt 1/3 failed")
			} else if mode == "retry-after" {
				until("sleeping until")
			} else {
				until("ready")
			}
			sig := syscall.SIGTERM
			if mode == "interrupt" {
				sig = syscall.SIGINT
			}
			require.NoError(t, child.Process.Signal(sig))
			if mode == "drain" || mode == "failure" {
				until("draining")
				assert.NotContains(t, output, "cleanup")
				_, err = fmt.Fprintln(stdin, "finish")
				require.NoError(t, err)
			} else if mode == "second" {
				until("waiting for active transfers")
				require.NoError(t, child.Process.Signal(syscall.SIGTERM))
			}
			for line := range lines {
				output = append(output, line)
			}
			err = child.Wait()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, strings.Join(output, "\n"))
			assert.Equal(t, 128+int(sig), exitErr.ExitCode())
			assert.Contains(t, output, "cleanup")
			assert.Equal(t, 1, strings.Count("\n"+strings.Join(output, "\n")+"\n", "\nattempt\n"))
			if mode == "failure" {
				assert.Contains(t, strings.Join(output, "\n"), "real transfer failure")
			}
		})
	}
}
