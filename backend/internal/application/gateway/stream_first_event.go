package gateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

const (
	defaultStreamFirstEventTimeout = 15 * time.Second
	maxFirstSSEPrefixBytes         = 2 << 20
)

var errStreamFirstEventTimeout = errors.New("Grok Build 首个流事件响应超时")

// forwardWithFirstSSEEventTimeout gives response headers and the first complete
// SSE data event one shared budget. The request context remains alive after the
// first event and is released only when the downstream closes the stream.
func forwardWithFirstSSEEventTimeout(
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
		cancel(errStreamFirstEventTimeout)
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
	if err != nil {
		timedOut := !stopTimer() && errors.Is(context.Cause(requestCtx), errStreamFirstEventTimeout)
		cancel(err)
		if timedOut {
			return nil, fmt.Errorf("%w: %v", errStreamFirstEventTimeout, err)
		}
		return nil, err
	}
	if response == nil {
		stopTimer()
		cancel(errors.New("上游返回空响应"))
		return nil, errors.New("上游返回空响应")
	}
	if errors.Is(context.Cause(requestCtx), errStreamFirstEventTimeout) {
		stopTimer()
		_ = response.Body.Close()
		return nil, errStreamFirstEventTimeout
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if !stopTimer() {
			_ = response.Body.Close()
			return nil, errStreamFirstEventTimeout
		}
		response.Body = &cancelOnCloseReadCloser{ReadCloser: response.Body, cancel: cancel}
		return response, nil
	}

	prefix, reader, readErr := readFirstSSEEvent(response.Body)
	timerStopped := stopTimer()
	timedOut := !timerStopped && errors.Is(context.Cause(requestCtx), errStreamFirstEventTimeout)
	if readErr != nil || timedOut {
		_ = response.Body.Close()
		if timedOut || errors.Is(context.Cause(requestCtx), errStreamFirstEventTimeout) {
			return nil, errStreamFirstEventTimeout
		}
		cancel(readErr)
		return nil, readErr
	}
	response.Body = &cancelOnCloseReadCloser{
		ReadCloser: &joinedReadCloser{
			Reader: io.MultiReader(bytes.NewReader(prefix), reader),
			Closer: response.Body,
		},
		cancel: cancel,
	}
	return response, nil
}

func readFirstSSEEvent(body io.Reader) ([]byte, *bufio.Reader, error) {
	if body == nil {
		return nil, nil, io.ErrUnexpectedEOF
	}
	reader := bufio.NewReaderSize(body, 32<<10)
	var prefix bytes.Buffer
	lineStart := 0
	eventHasData := false
	firstLine := true
	for {
		fragment, err := reader.ReadSlice('\n')
		if prefix.Len()+len(fragment) > maxFirstSSEPrefixBytes {
			return nil, reader, fmt.Errorf("首个 SSE 事件前缀超过 %d 字节", maxFirstSSEPrefixBytes)
		}
		_, _ = prefix.Write(fragment)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, reader, io.ErrUnexpectedEOF
			}
			return nil, reader, err
		}

		line := prefix.Bytes()[lineStart:prefix.Len()]
		lineStart = prefix.Len()
		line = bytes.TrimSuffix(line, []byte{'\n'})
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if firstLine {
			line = bytes.TrimPrefix(line, []byte{0xef, 0xbb, 0xbf})
			firstLine = false
		}
		if len(line) == 0 {
			if eventHasData {
				return append([]byte(nil), prefix.Bytes()...), reader, nil
			}
			eventHasData = false
			continue
		}
		if line[0] == ':' {
			continue
		}
		field := line
		if separator := bytes.IndexByte(line, ':'); separator >= 0 {
			field = line[:separator]
		}
		if bytes.Equal(field, []byte("data")) {
			eventHasData = true
		}
	}
}

type joinedReadCloser struct {
	io.Reader
	io.Closer
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
