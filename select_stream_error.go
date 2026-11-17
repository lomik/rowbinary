package rowbinary

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

// selectStreamError adds context to errors that end a 200 response early.
// ClickHouse reports a query that fails mid-stream by appending
// "__exception__" and the exception text to the body, then closing the
// connection without finishing the response.
func selectStreamError(err error, body *tailReader) error {
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	if msg := body.exception(); msg != "" {
		return fmt.Errorf("query failed on the server after streaming started: %s: %w", msg, err)
	}
	return fmt.Errorf("response ended unexpectedly (the query may have failed on the server after streaming started): %w", err)
}

// tailReader passes reads through and keeps the last tailSize bytes, so the
// exception ClickHouse appends to a failed stream can be reported.
type tailReader struct {
	r    io.Reader
	tail []byte
}

const tailSize = 16 << 10

var exceptionMarker = []byte("__exception__")

func (t *tailReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n >= tailSize {
		t.tail = append(t.tail[:0], p[n-tailSize:n]...)
	} else if n > 0 {
		t.tail = append(t.tail, p[:n]...)
		if extra := len(t.tail) - tailSize; extra > 0 {
			t.tail = append(t.tail[:0], t.tail[extra:]...)
		}
	}
	return n, err
}

// exception returns the text after the last "__exception__" marker, if any.
func (t *tailReader) exception() string {
	i := bytes.LastIndex(t.tail, exceptionMarker)
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(string(t.tail[i+len(exceptionMarker):]))
}
