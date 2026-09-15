package pacer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallContextCanceledWait(t *testing.T) {
	for _, connection := range []bool{false, true} {
		p := New(MaxConnectionsOption(1), CalculatorOption(NewDefault(MinSleep(time.Hour))))
		if connection {
			p.SetCalculator(&ZeroDelayCalculator{})
			p.state.SleepTime = 0
			<-p.connTokens
		} else {
			<-p.pacer
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := p.CallContext(ctx, func() (bool, error) {
			t.Error("已取消的等待不得发起请求")
			return false, nil
		})
		cancel()
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		if connection {
			assert.Empty(t, p.connTokens, "不能归还没有取得的 token")
			p.connTokens <- struct{}{}
		} else {
			p.pacer <- struct{}{}
			p.state.SleepTime = 0
		}
		require.NoError(t, p.CallContext(context.Background(), func() (bool, error) { return false, nil }))
		assert.Len(t, p.connTokens, 1)
	}
}

func TestCallContextRetryAndReentrant(t *testing.T) {
	p := New(MaxConnectionsOption(1), CalculatorOption(&ZeroDelayCalculator{}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	want := errors.New("retry")
	err := p.CallContext(ctx, func() (bool, error) {
		calls++
		require.NoError(t, p.Call(func() (bool, error) {
			return false, p.CallContext(ctx, func() (bool, error) { return false, nil })
		}))
		cancel()
		return true, want
	})
	assert.ErrorIs(t, err, want)
	assert.Equal(t, 1, calls)
	assert.Len(t, p.connTokens, 1)
}
