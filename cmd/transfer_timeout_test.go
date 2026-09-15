package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransferTimeoutFlagValues(t *testing.T) {
	for _, value := range []string{"30s", "2m", "1m30s", "0", "off"} {
		t.Run(value, func(t *testing.T) {
			flagSet := pflag.NewFlagSet(t.Name(), pflag.ContinueOnError)
			for _, option := range fs.ConfigOptionsInfo {
				if option.Name == "transfer_timeout" {
					option.Value = nil
					flags.AddFlagsFromOptions(flagSet, "", fs.Options{option})
					break
				}
			}
			require.NoError(t, flagSet.Parse([]string{"--transfer-timeout", value}))
		})
	}
}

func TestTransferTimeoutHighLevelRetry(t *testing.T) {
	const childEnv = "RCLONE_TEST_TRANSFER_TIMEOUT_CHILD"
	if mode := os.Getenv(childEnv); mode != "" {
		ctx := context.Background()
		ci := fs.GetConfig(ctx)
		ci.Retries = 3
		ci.TransferTimeout = fs.Duration(time.Millisecond)
		accounting.Start(ctx)
		attempts := 0
		Run(true, false, &cobra.Command{Use: "copy"}, func() error {
			attempts++
			fmt.Printf("attempt=%d\n", attempts)
			if attempts > 1 {
				return nil
			}
			if mode != "ordinary" {
				transferCtx, cancel := fs.WithTransferTimeout(ctx)
				<-transferCtx.Done()
				err := fs.CountError(ctx, fs.TransferError(transferCtx, nil))
				cancel()
				fmt.Println("remaining-object-completed")
				if mode == "timeout" {
					return err
				}
			}
			return fs.CountError(ctx, errors.New("ordinary retryable error"))
		})
		return
	}
	for _, mode := range []string{"timeout", "mixed", "ordinary"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTransferTimeoutHighLevelRetry$")
			command.Env = append(os.Environ(), childEnv+"="+mode, "RCLONE_CONFIG=/notfound")
			out, err := command.CombinedOutput()
			if mode == "ordinary" {
				require.NoError(t, err, string(out))
				assert.Equal(t, 2, strings.Count(string(out), "attempt="))
			} else {
				var exitErr *exec.ExitError
				require.ErrorAs(t, err, &exitErr, string(out))
				assert.NotZero(t, exitErr.ExitCode())
				if mode == "timeout" {
					assert.Equal(t, 6, exitErr.ExitCode())
				}
				assert.Equal(t, 1, strings.Count(string(out), "attempt="), string(out))
				assert.Contains(t, string(out), "remaining-object-completed")
				assert.Contains(t, string(out), "preserving errors without high-level retries")
			}
		})
	}
}
