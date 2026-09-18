package fs

import (
	"context"
	"errors"
	"sync"

	"github.com/rclone/rclone/fs/fserrors"
)

type gracefulShutdownKey struct{}
type gracefulInputKey struct{}
type activeTransferKey struct{}

var errGracefulShutdown = errors.New("graceful shutdown requested")

// GracefulShutdown stops admission of new objects without cancelling active transfers.
// A controller belongs to one command, including all of its high-level retries.
type GracefulShutdown struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

// WithGracefulShutdown attaches a new shutdown controller to ctx.
func WithGracefulShutdown(ctx context.Context) (context.Context, *GracefulShutdown) {
	s := &GracefulShutdown{}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return context.WithValue(ctx, gracefulShutdownKey{}, s), s
}

// GetGracefulShutdown returns the command's shutdown controller, or nil if disabled.
func GetGracefulShutdown(ctx context.Context) *GracefulShutdown {
	s, _ := ctx.Value(gracefulShutdownKey{}).(*GracefulShutdown)
	return s
}

// Stop prevents further object starts and reports whether this is the first request.
func (s *GracefulShutdown) Stop() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Stopped() {
		return false
	}
	s.cancel()
	return true
}

// Done closes on a stop request. A nil controller returns a nil channel.
func (s *GracefulShutdown) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.ctx.Done()
}

// Stopped reports whether a stop has been requested, or false for a nil controller.
func (s *GracefulShutdown) Stopped() bool {
	return s != nil && s.ctx.Err() != nil
}

// WithGracefulInput cancels listing and checking on a graceful stop.
// StartTransfer promotes this context back to its parent on admission, preserving
// the parent's cancellation and deadlines. The caller must call cancel.
func WithGracefulInput(ctx context.Context) (context.Context, context.CancelFunc) {
	s := GetGracefulShutdown(ctx)
	if s == nil {
		return ctx, func() {}
	}
	parent := ctx
	ctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.ctx, func() { cancel(errGracefulShutdown) })
	if s.Stopped() {
		cancel(errGracefulShutdown)
	}
	return context.WithValue(ctx, gracefulInputKey{}, parent), func() {
		stop()
		cancel(context.Canceled)
	}
}

// IsGracefulStop reports cancellation of listing or checking by a graceful stop.
// It does not match failures or cancellation of admitted transfers.
func IsGracefulStop(ctx context.Context, err error) bool {
	return context.Cause(ctx) == errGracefulShutdown && errors.Is(err, context.Canceled) && !fserrors.IsCounted(err)
}

// StartTransfer admits an object atomically with respect to Stop.
// A false result means the caller must skip the object without reporting an error.
// Nested copies must reuse the returned context for the admitted object's lifetime.
func StartTransfer(ctx context.Context) (context.Context, bool) {
	s := GetGracefulShutdown(ctx)
	if s == nil || ctx.Value(activeTransferKey{}) == s {
		return ctx, true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Stopped() {
		return ctx, false
	}
	if parent, ok := ctx.Value(gracefulInputKey{}).(context.Context); ok {
		ctx = parent
	}
	return context.WithValue(ctx, activeTransferKey{}, s), true
}
