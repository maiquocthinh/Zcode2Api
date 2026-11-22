// Package proxy implements the data plane: the
// Anthropic-compatible client endpoints, the failover attempt loop around the
// single upstream (UPSTREAM_URL), and the first-event lookahead that decides
// failover before any byte reaches the client.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrFirstEventTimeout is returned by ReadFirstEvent when the upstream did not
// deliver a complete first SSE event within the timeout. Before COMMIT a
// timeout is retryable with another key.
var ErrFirstEventTimeout = errors.New("proxy: timeout waiting for the first SSE event")

// errFirstEventEOF wraps a clean EOF seen before any complete event: the
// attempt may be retried like any other upstream-unavailable failure.
var errFirstEventEOF = errors.New("proxy: upstream closed before the first SSE event")

// firstEventResult carries the outcome of the blocking read goroutine.
type firstEventResult struct {
	buf   []byte
	event string
	data  []byte
	err   error
}

// Deliberately not a Peek(N): a fixed-size peek hangs when the first chunk is
// smaller than the window. The read is driven by the SSE boundary instead.
// The bufio.Reader is consumed by an internal goroutine so the timeout and
// the client context can abandon a blocked read; on error the caller closes
// the response body, which unblocks it.
func ReadFirstEvent(ctx context.Context, r *bufio.Reader, maxBytes int, timeout time.Duration) (buf []byte, event string, data []byte, err error) {
	if maxBytes <= 0 {
		maxBytes = 1
	}
	if timeout <= 0 {
		timeout = 1 * time.Second
	}

	results := make(chan firstEventResult, 1)
	go func() {
		b, ev, d, e := readFirstEvent(r, maxBytes)
		results <- firstEventResult{buf: b, event: ev, data: d, err: e}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case res := <-results:
		return res.buf, res.event, res.data, res.err
	case <-timer.C:
		return nil, "", nil, ErrFirstEventTimeout
	case <-ctx.Done():
		return nil, "", nil, context.Canceled
	}
}

// readFirstEvent is the blocking core of ReadFirstEvent: it parses SSE lines
// until the first non-ping event completes, or the maxBytes budget is spent.
func readFirstEvent(r *bufio.Reader, maxBytes int) (buf []byte, event string, data []byte, err error) {
	var (
		line      []byte
		evName    string
		dataLines [][]byte
		sawField  bool
	)

	parseLine := func(raw []byte) {
		trimmed := bytes.TrimSuffix(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\r'})
		field, value, _ := bytes.Cut(trimmed, []byte{':'})
		switch string(field) {
		case "event":
			evName = string(bytes.TrimSpace(value))
			sawField = true
		case "data":
			v := bytes.TrimPrefix(value, []byte{' '})
			dataLines = append(dataLines, bytes.Clone(v))
			sawField = true
		}

	}

	for {
		frag, ferr := r.ReadSlice('\n')
		switch {
		case errors.Is(ferr, bufio.ErrBufferFull):
			line = append(line, frag...)
			if len(buf)+len(line) >= maxBytes {
				return append(buf, line...), "", nil, nil
			}
			continue

		case errors.Is(ferr, io.EOF):
			if len(buf)+len(line)+len(frag) >= maxBytes {
				buf = append(append(buf, line...), frag...)
				return buf, "", nil, nil
			}
			return buf, "", nil, errFirstEventEOF

		case ferr != nil:
			return buf, "", nil, fmt.Errorf("proxy: read first event: %w", ferr)
		}

		line = append(line, frag...)
		buf = append(buf, line...)
		raw := line
		line = nil
		blank := isBlankLine(raw)
		parseLine(raw)

		if blank {
			if sawField && evName != "ping" {
				return buf, evName, bytes.Join(dataLines, []byte{'\n'}), nil
			}

			evName, dataLines, sawField = "", nil, false
		}

		if len(buf) >= maxBytes {
			return buf, "", nil, nil
		}
	}
}

// isBlankLine reports whether the just-completed line (including its '\n') is
// blank — the SSE event delimiter. CRLF line endings are tolerated.
func isBlankLine(line []byte) bool {
	l := bytes.TrimSuffix(line, []byte{'\n'})
	l = bytes.TrimSuffix(l, []byte{'\r'})
	return len(l) == 0
}
