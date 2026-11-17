package rowbinary

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// shortWriter breaks the io.Writer contract: it writes at most max bytes per
// call and reports no error.
type shortWriter struct {
	buf bytes.Buffer
	max int
}

func (s *shortWriter) Write(p []byte) (int, error) {
	n := min(len(p), s.max)
	s.buf.Write(p[:n])
	return n, nil
}

func TestShortWritesAreErrors(t *testing.T) {
	for _, max := range []int{0, 2} {
		cases := map[string]func(Writer) error{
			"String":   func(w Writer) error { return String.Write(w, "hello") },
			"UInt64":   func(w Writer) error { return UInt64.Write(w, 1) },
			"UInt8":    func(w Writer) error { return UInt8.Write(w, 1) }, // WriteByte path
			"Nullable": func(w Writer) error { return Nullable(String).Write(w, nil) },
		}
		for name, write := range cases {
			sw := &shortWriter{max: max}
			err := write(NewWriter(sw))
			if max >= 1 && name == "UInt8" || max >= 1 && name == "Nullable" {
				// One byte fits: not a short write.
				if err != nil {
					t.Errorf("max %d, %s: %v", max, name, err)
				}
				continue
			}
			if !errors.Is(err, io.ErrShortWrite) {
				t.Errorf("max %d, %s: err = %v, want io.ErrShortWrite", max, name, err)
			}
		}
	}

	// The RowBinaryWithNamesAndTypes header goes through the same writer.
	fw := NewFormatWriter(&shortWriter{max: 3}, RowBinaryWithNamesAndTypes, C("value", String))
	if err := Write(fw, String, "x"); !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("format header: err = %v, want io.ErrShortWrite", err)
	}
}

func TestFullWritesAreUnaffected(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := String.Write(w, "hello"); err != nil {
		t.Fatal(err)
	}
	if err := UInt8.Write(w, 7); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), []byte{5, 'h', 'e', 'l', 'l', 'o', 7}) {
		t.Fatalf("wrote % x", buf.Bytes())
	}
}
