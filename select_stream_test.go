package rowbinary

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// twoColumnRows encodes n rows of (UInt32, String) in RowBinaryWithNamesAndTypes.
func twoColumnRows(t *testing.T, n int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewFormatWriter(&buf, RowBinaryWithNamesAndTypes, C("a", UInt32), C("b", String))
	for i := range n {
		if err := Write(w, UInt32, uint32(i)); err != nil {
			t.Fatal(err)
		}
		if err := Write(w, String, "row"); err != nil {
			t.Fatal(err)
		}
	}
	return buf.Bytes()
}

// readRows reads (UInt32, String) rows until Next or Scan fails, then returns
// the row count and the reader error, like a callback that ignores errors.
func readRows(r *FormatReader) int {
	n := 0
	for r.Next() {
		var a uint32
		var b string
		if Scan(r, UInt32, &a) != nil || Scan(r, String, &b) != nil {
			break
		}
		n++
	}
	return n
}

func TestFormatReaderCleanEndAndTruncation(t *testing.T) {
	full := twoColumnRows(t, 3)

	r := NewFormatReader(bytes.NewReader(full), RowBinaryWithNamesAndTypes)
	if n := readRows(r); n != 3 || r.Err() != nil {
		t.Fatalf("full stream: %d rows, err %v", n, r.Err())
	}
	var a uint32
	if err := Scan(r, UInt32, &a); err != io.EOF {
		t.Fatalf("Scan at clean end = %v, want io.EOF", err)
	}

	for name, cut := range map[string]int{
		"inside first column":  len(full) - 6, // last row is 8 bytes: UInt32 (4) + String "row" (1+3)
		"between columns":      len(full) - 4, // last row: UInt32 present, String missing
		"inside second column": len(full) - 2,
	} {
		r := NewFormatReader(bytes.NewReader(full[:cut]), RowBinaryWithNamesAndTypes)
		n := readRows(r)
		if !errors.Is(r.Err(), io.ErrUnexpectedEOF) {
			t.Errorf("%s: %d rows, err %v, want io.ErrUnexpectedEOF", name, n, r.Err())
		}
	}
}

func TestSelectReportsTruncatedStream(t *testing.T) {
	body := twoColumnRows(t, 3)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body[:len(body)-2])
	}))
	defer s.Close()

	err := NewClient(WithDSN(s.URL)).Select(t.Context(), "SELECT", WithFormatReader(func(r *FormatReader) error {
		readRows(r)
		return nil // ignores r.Err(), as many callbacks do
	}))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Select = %v, want io.ErrUnexpectedEOF", err)
	}
}

// clickhouseMidStreamFailure mimics ClickHouse 25.9 when a query fails after
// streaming started: status 200, chunked rows, an __exception__ block, then the
// connection is closed without the terminating chunk.
func clickhouseMidStreamFailure(t *testing.T, rows []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		chunk := func(p []byte) { fmt.Fprintf(buf, "%x\r\n%s\r\n", len(p), p) }
		buf.WriteString("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/octet-stream\r\n\r\n")
		chunk(rows)
		chunk([]byte("__exception__\r\nCode: 395. DB::Exception: Value passed to 'throwIf' function is non-zero\n"))
		buf.Flush()
	}))
}

func TestSelectReportsMidStreamServerFailure(t *testing.T) {
	s := clickhouseMidStreamFailure(t, twoColumnRows(t, 2))
	defer s.Close()
	c := NewClient(WithDSN(s.URL))

	if err := c.Select(t.Context(), "SELECT"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("Select without reader = %v, want io.ErrUnexpectedEOF", err)
	}
	err := c.Select(t.Context(), "SELECT", WithFormatReader(func(r *FormatReader) error {
		readRows(r)
		return nil
	}))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("Select with reader = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestSelectWithoutReaderSucceedsOnCompleteResponse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(bytes.Repeat([]byte{1}, 3<<20))
	}))
	defer s.Close()
	if err := NewClient(WithDSN(s.URL)).Select(t.Context(), "SELECT"); err != nil {
		t.Fatal(err)
	}
}

func TestExternalDataSingleClosingBoundary(t *testing.T) {
	var body []byte
	var boundary string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		boundary = strings.TrimPrefix(r.Header.Get("Content-Type"), "multipart/form-data; boundary=")
		body, _ = io.ReadAll(r.Body)
	}))
	defer s.Close()

	err := NewClient(WithDSN(s.URL)).Select(t.Context(), "SELECT * FROM ext",
		WithExternalData("ext", C("x", UInt8), WithFormatWriter(func(w *FormatWriter) error {
			return Write(w, UInt8, 1)
		})))
	if err != nil {
		t.Fatal(err)
	}
	if n := bytes.Count(body, []byte("--"+boundary+"--")); n != 1 {
		t.Fatalf("closing boundary appears %d times, want 1", n)
	}
	if !bytes.HasSuffix(body, []byte("--"+boundary+"--\r\n")) {
		t.Fatalf("body does not end with the closing boundary: %q", body[max(0, len(body)-80):])
	}
}
