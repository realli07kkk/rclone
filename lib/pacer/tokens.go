// Tokens for controlling concurrency

package pacer

import "context"

// TokenDispenser is for controlling concurrency
type TokenDispenser struct {
	tokens chan struct{}
}

// NewTokenDispenser makes a pool of n tokens
func NewTokenDispenser(n int) *TokenDispenser {
	td := &TokenDispenser{
		tokens: make(chan struct{}, n),
	}
	// Fill up the upload tokens
	for range n {
		td.tokens <- struct{}{}
	}
	return td
}

// Get gets a token from the pool - don't forget to return it with Put
func (td *TokenDispenser) Get() {
	<-td.tokens
}

// GetContext 可取消地获取 token；成功后调用者必须用 Put 归还。
func (td *TokenDispenser) GetContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-td.tokens:
		return nil
	}
}

// Put returns a token
func (td *TokenDispenser) Put() {
	td.tokens <- struct{}{}
}
