package proxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// readFirstEventOf feeds s through ReadFirstEvent and flattens the result.
func readFirstEventOf(t *testing.T, s string, maxBytes int, timeout time.Duration) ([]byte, string, string, error) {
	t.Helper()
	buf, ev, data, err := ReadFirstEvent(context.Background(),
		bufio.NewReader(strings.NewReader(s)), maxBytes, timeout)
	return buf, ev, string(data), err
}

// TestReadFirstEventBoundary covers the happy path: the read stops exactly at
// the "\n\n" boundary of the first real event, everything consumed is
// returned for replay, and bytes after the boundary stay in the reader.
func TestReadFirstEventBoundary(t *testing.T) {
	const rest = "event: content_block_delta\ndata: {\"d\":2}\n\n"
	buf, ev, data, err := readFirstEventOf(t,
		"event: message_start\ndata: {\"type\":\"message_start\"}\n\n"+rest,
		65536, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev != "message_start" {
		t.Errorf("event = %q, want message_start", ev)
	}
	if data != "{\"type\":\"message_start\"}" {
		t.Errorf("data = %q", data)
	}
	if want := "event: message_start\ndata: {\"type\":\"message_start\"}\n\n"; string(buf) != want {
		t.Errorf("buf = %q, want %q", buf, want)
	}
	// The remainder is still readable from a fresh reader over the same tail.
	rd := bufio.NewReader(iotest.OneByteReader(strings.NewReader(rest)))
	_, ev2, data2, err2 := ReadFirstEvent(context.Background(), rd, 65536, time.Second)
	if err2 != nil || ev2 != "content_block_delta" || string(data2) != "{\"d\":2}" {
		t.Errorf("remainder read: ev=%q data=%q err=%v", ev2, data2, err2)
	}
}

// TestReadFirstEventSkipsPingAndComments: comment lines and ping events at
// the head of the stream are skipped as events but kept in buf; the read
// continues to the first non-ping event.
func TestReadFirstEventSkipsPingAndComments(t *testing.T) {
	input := ": keepalive\n" +
		"event: ping\ndata: {\"type\":\"ping\"}\n\n" +
		"event: message_start\ndata: {\"type\":\"message_start\"}\n\n"
	buf, ev, data, err := readFirstEventOf(t, input, 65536, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev != "message_start" {
		t.Errorf("event = %q, want message_start", ev)
	}
	if data != "{\"type\":\"message_start\"}" {
		t.Errorf("data = %q", data)
	}
	// buf must hold every consumed byte: comments, ping event, and the first
	// real event — ready for verbatim replay at COMMIT.
	if !strings.Contains(string(buf), ": keepalive") ||
		!strings.Contains(string(buf), "event: ping") ||
		!strings.Contains(string(buf), "event: message_start") {
		t.Errorf("buf %q lost ping/comment bytes", buf)
	}
}

// TestReadFirstEventSplitChunks: the event may arrive split across many small
// reads — the parser must not require any fixed-size peek window.
func TestReadFirstEventSplitChunks(t *testing.T) {
	s := "event: message_start\ndata: {\"usage\":{\"input_tokens\":10}}\n\n"
	buf, ev, data, err := ReadFirstEvent(context.Background(),
		bufio.NewReader(iotest.OneByteReader(strings.NewReader(s))), 65536, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev != "message_start" || string(data) != "{\"usage\":{\"input_tokens\":10}}" {
		t.Errorf("ev=%q data=%q", ev, data)
	}
	if string(buf) != s {
		t.Errorf("buf = %q, want %q", buf, s)
	}
}

// TestReadFirstEventMultipleDataLines: several data lines of one event are
// joined with "\n" (standard SSE framing).
func TestReadFirstEventMultipleDataLines(t *testing.T) {
	_, ev, data, err := readFirstEventOf(t,
		"event: error\ndata: {\"error\":{\"code\":\ndata: \"1308\"}}\n\n", 65536, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev != "error" {
		t.Errorf("event = %q", ev)
	}
	if data != "{\"error\":{\"code\":\n\"1308\"}}" {
		t.Errorf("data = %q", data)
	}
}

// TestReadFirstEventCRLF: CRLF line endings are tolerated.
func TestReadFirstEventCRLF(t *testing.T) {
	_, ev, data, err := readFirstEventOf(t, "event: message_start\r\ndata: {\"a\":1}\r\n\r\n", 65536, time.Second)
	if err != nil || ev != "message_start" || data != "{\"a\":1}" {
		t.Errorf("ev=%q data=%q err=%v", ev, data, err)
	}
}

// TestReadFirstEventMaxBytesOverflow: when maxBytes is spent without any
// event boundary the read reports ok (event ""/data nil) and returns every
// byte consumed so the caller can COMMIT and replay them.
func TestReadFirstEventMaxBytesOverflow(t *testing.T) {
	body := strings.Repeat("x", 100) // no "\n\n" anywhere
	buf, ev, data, err := readFirstEventOf(t, body, 32, time.Second)
	if err != nil {
		t.Fatalf("overflow must not be an error: %v", err)
	}
	if ev != "" || data != "" {
		t.Errorf("overflow should carry no event/data, got ev=%q data=%q", ev, data)
	}
	if len(buf) < 32 {
		t.Errorf("buf = %d bytes, want at least maxBytes", len(buf))
	}
	if !strings.HasPrefix(body, string(buf)) {
		t.Errorf("buf %q is not a prefix of the body — bytes would be lost", buf)
	}
}

// TestReadFirstEventTimeout: a blocked upstream is abandoned after the
// timeout with ErrFirstEventTimeout — still retryable before COMMIT.
func TestReadFirstEventTimeout(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	start := time.Now()
	_, _, _, err := ReadFirstEvent(context.Background(), bufio.NewReader(pr), 65536, 80*time.Millisecond)
	if !errors.Is(err, ErrFirstEventTimeout) {
		t.Fatalf("err = %v, want ErrFirstEventTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("timeout took %v, the read was not abandoned", elapsed)
	}
}

// TestReadFirstEventContextCanceled: a canceled client context aborts the
// wait with context.Canceled (no retry afterwards).
func TestReadFirstEventContextCanceled(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, _, _, err := ReadFirstEvent(ctx, bufio.NewReader(pr), 65536, 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestReadFirstEventEOFBeforeBoundary: a clean EOF before any complete event
// is a transport failure — nothing was committed, so the attempt may retry.
func TestReadFirstEventEOFBeforeBoundary(t *testing.T) {
	_, _, _, err := readFirstEventOf(t, "event: message_start\ndata: {\"partial\"", 65536, time.Second)
	if err == nil || errors.Is(err, ErrFirstEventTimeout) {
		t.Fatalf("err = %v, want a transport error (errFirstEventEOF)", err)
	}
	if !errors.Is(err, errFirstEventEOF) {
		t.Errorf("err = %v, want errFirstEventEOF", err)
	}
}
