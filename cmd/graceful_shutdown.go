package cmd

import (
	"context"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/atexit"
	"github.com/spf13/cobra"
)

// EnableGracefulShutdown lets the command drain active objects on its first SIGTERM.
// It returns a function which unregisters the signal interceptor.
func EnableGracefulShutdown(command *cobra.Command) func() {
	ctx := command.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, shutdown := fs.WithGracefulShutdown(ctx)
	command.SetContext(ctx)
	return atexit.OnTerminate(func() bool {
		if !shutdown.Stop() {
			return false
		}
		fs.Logf(nil, "SIGTERM received: stopping new objects, waiting for active transfers to finish")
		return true
	})
}

func retrySleep(shutdown *fs.GracefulShutdown, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-shutdown.Done():
	}
}
