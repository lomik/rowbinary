package rowbinary

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSwap32MultipleWords(t *testing.T) {
	b := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	swap32(b)
	if want := []byte{4, 3, 2, 1, 8, 7, 6, 5}; !bytes.Equal(b, want) {
		t.Fatalf("swap32 = % x, want % x", b, want)
	}
}

func TestIntervalYearBinaryKind(t *testing.T) {
	// ClickHouse 25.9 encodes IntervalYear as 22 0a in binary type headers.
	if got, want := IntervalYear.Binary(), []byte{0x22, 0x0a}; !bytes.Equal(got, want) {
		t.Fatalf("IntervalYear.Binary() = % x, want % x", got, want)
	}
	tp, err := DecodeBinaryType(NewReader(bytes.NewReader([]byte{0x22, 0x0a})))
	if err != nil {
		t.Fatal(err)
	}
	if !Eq(tp, IntervalYear) {
		t.Fatalf("decoded %s, want IntervalYear", tp)
	}
}

func TestFormatHeaderOverrideIsDeterministic(t *testing.T) {
	var got []string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Values("X-Clickhouse-Format")...)
	}))
	defer s.Close()
	c := NewClient(WithDSN(s.URL))
	for range 50 {
		if err := c.Select(t.Context(), "SELECT 1", WithHeader("X-ClickHouse-Format", "JSON")); err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range got {
		if v != "JSON" {
			t.Fatalf("format header values = %v, want only JSON", got)
		}
	}
	if len(got) != 50 {
		t.Fatalf("got %d format header values, want 50", len(got))
	}
}

func TestNilQueryOptionsAreSkipped(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer s.Close()
	c := NewClient(WithDSN(s.URL))
	if err := c.Select(t.Context(), "SELECT 1", nil); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if err := c.Exec(t.Context(), "SELECT 1", nil); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := c.Insert(t.Context(), "t", nil, WithBodyWriter(func(w io.Writer) error { return nil })); err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

type unreadFailingReader struct{ Reader }

var errUnreadFailed = errors.New("unread failed")

func (unreadFailingReader) UnreadByte() error { return errUnreadFailed }

func TestFormatReaderNextKeepsUnreadByteError(t *testing.T) {
	r := NewFormatReader(unreadFailingReader{NewReader(bytes.NewReader([]byte{1}))}, C("x", UInt8))
	if r.Next() {
		t.Fatal("Next() = true, want false")
	}
	if !errors.Is(r.Err(), errUnreadFailed) {
		t.Fatalf("Err() = %v, want %v", r.Err(), errUnreadFailed)
	}
}
