package fs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rclone/rclone/fs/fserrors"
)

// ErrorTransferTimeout 表示单个对象已耗尽复制时间预算。
var ErrorTransferTimeout = errors.New("transfer timeout exceeded")

type transferTimeoutKey struct{}
type transferCleanupKey struct{}

type transferTimeout struct {
	ctx             context.Context
	parent          context.Context
	deadline        time.Time
	limit           Duration
	mu              sync.Mutex
	cleanupDeadline time.Time
}

// WithTransferTimeout 为一个出队对象建立时限；嵌套复制复用同一预算。
// 返回的 cancel 必须在该对象的同步复制和收尾结束后调用。
func WithTransferTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	limit := GetConfig(ctx).TransferTimeout
	if HasTransferTimeout(ctx) || limit <= 0 || limit == DurationOff {
		return ctx, func() {}
	}
	s := &transferTimeout{parent: ctx, limit: limit, deadline: time.Now().Add(time.Duration(limit))}
	// 父截止优先，避免相同 deadline 的两个 timer 竞争错误分类。
	cause := ErrorTransferTimeout
	if deadline, ok := ctx.Deadline(); ok && !deadline.After(s.deadline) {
		cause = context.DeadlineExceeded
	}
	var cancel context.CancelFunc
	s.ctx, cancel = context.WithDeadlineCause(ctx, s.deadline, cause)
	return context.WithValue(s.ctx, transferTimeoutKey{}, s), cancel
}

// HasTransferTimeout 报告 ctx 是否属于启用了单对象时限的复制。
func HasTransferTimeout(ctx context.Context) bool {
	return ctx.Value(transferTimeoutKey{}) != nil
}

// TransferError 在对象预算耗尽时返回不可自动重试的错误，保留已计数的失败。
// 内部 errgroup 的取消不改变对象时限的归属；父取消保留原错误语义。
func TransferError(ctx context.Context, err error) error {
	s, ok := ctx.Value(transferTimeoutKey{}).(*transferTimeout)
	if !ok || errors.Is(err, ErrorTransferTimeout) || fserrors.IsCounted(err) {
		return err
	}
	cause := context.Cause(s.ctx)
	expired := errors.Is(cause, ErrorTransferTimeout)
	if cause == nil && s.parent.Err() == nil && !time.Now().Before(s.deadline) {
		deadline, hasDeadline := s.parent.Deadline()
		expired = !hasDeadline || deadline.After(s.deadline)
	}
	if expired {
		return fserrors.NoRetryError(fserrors.NoLowLevelRetryError(&transferTimeoutError{
			limit: s.limit,
			cause: err,
		}))
	}
	if err == nil {
		return s.ctx.Err()
	}
	return err
}

type transferTimeoutError struct {
	limit Duration
	cause error
}

func (e *transferTimeoutError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%v (limit=%s): %v", ErrorTransferTimeout, e.limit, e.cause)
	}
	return fmt.Sprintf("%v (limit=%s)", ErrorTransferTimeout, e.limit)
}

func (e *transferTimeoutError) Is(target error) bool { return target == ErrorTransferTimeout }
func (e *transferTimeoutError) Unwrap() error        { return e.cause }

// TransferCleanupContext 为同一个对象的远端清理共用最多 30 秒，脱离业务取消。
// 仅用于清理已有资源，不能用于续传或提交；调用者必须调用返回的 cancel。
// 未启用单对象时限时返回原 context。
func TransferCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Value(transferCleanupKey{}) != nil {
		return ctx, func() {}
	}
	s, ok := ctx.Value(transferTimeoutKey{}).(*transferTimeout)
	if !ok {
		return ctx, func() {}
	}
	s.mu.Lock()
	if s.cleanupDeadline.IsZero() {
		s.cleanupDeadline = time.Now().Add(30 * time.Second)
	}
	deadline := s.cleanupDeadline
	s.mu.Unlock()
	cleanupCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	return context.WithValue(cleanupCtx, transferCleanupKey{}, true), cancel
}
