package rowbinary

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
)

// sliceReader is a Reader whose Peek returns slices of the caller's own data,
// as a zero-copy Reader implementation may.
type sliceReader struct {
	data []byte
	pos  int
}

func (s *sliceReader) Read(p []byte) (int, error) {
	n := copy(p, s.data[s.pos:])
	s.pos += n
	return n, nil
}
func (s *sliceReader) ReadByte() (byte, error) { b := s.data[s.pos]; s.pos++; return b, nil }
func (s *sliceReader) UnreadByte() error       { s.pos--; return nil }
func (s *sliceReader) Peek(n int) ([]byte, error) {
	return s.data[s.pos : s.pos+n], nil
}
func (s *sliceReader) Discard(n int) (int, error) { s.pos += n; return n, nil }

func TestUUIDScanDoesNotModifyInput(t *testing.T) {
	want := uuid.MustParse("258b07b7-daa1-4c80-8062-58a2e07c2601")
	var enc bytes.Buffer
	if err := UUID.Write(NewWriter(&enc), want); err != nil {
		t.Fatal(err)
	}
	data := bytes.Clone(enc.Bytes())
	orig := bytes.Clone(data)

	var got uuid.UUID
	if err := UUID.Scan(&sliceReader{data: data}, &got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	if !bytes.Equal(data, orig) {
		t.Fatalf("Scan modified the input: % x, was % x", data, orig)
	}
	// Reading the same bytes again gives the same UUID.
	if err := UUID.Scan(&sliceReader{data: data}, &got); err != nil || got != want {
		t.Fatalf("second read: %s, %v", got, err)
	}
}
