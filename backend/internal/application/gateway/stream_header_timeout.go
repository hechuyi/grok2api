package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

const defaultStreamResponseHeaderTimeout = 15 * time.Second

var errStreamResponseHeaderTimeout = errors.New("Grok Build 响应头超时")

// forwardWithResponseHeaderTimeout limits only the wait for HTTP response
// headers. Once headers arrive, the official streaming body is passed through
// untouched and its request context lives until the downstream closes it.
func forwardWithResponseHeaderTimeout(
	ctx context.Context,
	timeout time.Duration,
	forward func(context.Context) (*provider.Response, error),
) (*provider.Response, error) {
	if timeout <= 0 {
		return forward(ctx)
	}
	requestCtx, cancel := context.WithCancelCause(ctx)
	timerDone := make(chan struct{})
	timer := time.AfterFunc(timeout, func() {
		cancel(errStreamResponseHeaderTimeout)
		close(timerDone)
	})
	stopTimer := func() bool {
		if timer.Stop() {
			return true
		}
		<-timerDone
		return false
	}

	response, err := forward(requestCtx)
	timerStopped := stopTimer()
	timedOut := !timerStopped && errors.Is(context.Cause(requestCtx), errStreamResponseHeaderTimeout)
	if err != nil {
		cancel(err)
		if ctx.Err() == nil && (timedOut || isTimeoutError(err)) {
			return nil, fmt.Errorf("%w: %v", errStreamResponseHeaderTimeout, err)
		}
		return nil, err
	}
	if response == nil {
		cancel(errors.New("上游返回空响应"))
		return nil, errors.New("上游返回空响应")
	}
	if timedOut {
		if response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, errStreamResponseHeaderTimeout
	}
	if response.Body == nil {
		cancel(context.Canceled)
		return response, nil
	}
	response.Body = &cancelOnCloseReadCloser{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

func isTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}

type cancelOnCloseReadCloser struct {
	io.ReadCloser
	cancel context.CancelCauseFunc
	once   sync.Once
}

func (b *cancelOnCloseReadCloser) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { b.cancel(context.Canceled) })
	return err
}
