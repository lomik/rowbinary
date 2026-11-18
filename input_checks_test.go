package rowbinary

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"
)

func TestInsertWithoutWriterFails(t *testing.T) {
	hit := false
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit = true }))
	defer s.Close()
	err := NewClient(WithDSN(s.URL)).Insert(t.Context(), "t")
	if err == nil {
		t.Fatal("Insert without a writer returned nil")
	}
	if hit {
		t.Fatal("Insert without a writer sent a request")
	}
}

func TestScanWithZeroColumnsFails(t *testing.T) {
	var x uint8
	r := NewFormatReader(bytes.NewReader([]byte{0}), RowBinaryWithNames)
	if err := r.Scan(&x); err == nil {
		t.Fatal("r.Scan with zero columns returned nil")
	}
	r = NewFormatReader(bytes.NewReader([]byte{0}), RowBinaryWithNames)
	if err := Scan(r, UInt8, &x); err == nil {
		t.Fatal("Scan[T] with zero columns returned nil")
	}
}

func TestFormatWriterRejectsUnknownFormat(t *testing.T) {
	var buf bytes.Buffer
	w := NewFormatWriter(&buf, Format(7), C("x", UInt8))
	if err := Write(w, UInt8, 5); err == nil {
		t.Fatal("unknown format accepted")
	}
	if buf.Len() != 0 {
		t.Fatalf("unknown format wrote % x", buf.Bytes())
	}
}

func TestDateUpperBound(t *testing.T) {
	var buf bytes.Buffer
	if err := Date.Write(NewWriter(&buf), ValueDate{Year: 2149, Month: 6, Day: 6}); err != nil {
		t.Fatalf("2149-06-06 (max Date) rejected: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), []byte{0xff, 0xff}) {
		t.Fatalf("2149-06-06 encoded as % x, want ff ff", buf.Bytes())
	}
	if err := Date.Write(NewWriter(&buf), ValueDate{Year: 2149, Month: 6, Day: 7}); err == nil {
		t.Fatal("2149-06-07 accepted")
	}
	if err := Date.Write(NewWriter(&buf), ValueDate{Year: 2150, Month: 1, Day: 1}); err == nil {
		t.Fatal("2150-01-01 accepted")
	}
}

func TestDateTime64PrecisionValidation(t *testing.T) {
	for _, header := range [][]byte{
		{0x13, 10},
		{0x13, 0x40},
		append([]byte{0x14, 0x40}, StringEncode("UTC")...),
	} {
		if tp, err := DecodeBinaryType(NewReader(bytes.NewReader(header))); err == nil {
			t.Errorf("binary header % x decoded as %s", header, tp)
		}
	}
	for _, s := range []string{"DateTime64(10)", "DateTime64(64, 'UTC')", "DateTime64(256)", "DateTime64(-1)"} {
		if tp, err := DecodeStringType(s); err == nil {
			t.Errorf("%s decoded as %s", s, tp)
		}
	}
	for _, s := range []string{"DateTime64(9)", "DateTime64(0, 'UTC')"} {
		if _, err := DecodeStringType(s); err != nil {
			t.Errorf("%s rejected: %v", s, err)
		}
	}

	// Directly constructed types fail instead of panicking or returning garbage.
	for _, tp := range []Type[time.Time]{DateTime64(10), DateTime64(64), DateTime64TZ(64, "UTC")} {
		var v time.Time
		if err := tp.Scan(NewReader(bytes.NewReader(make([]byte, 8))), &v); err == nil {
			t.Errorf("%s Scan accepted", tp)
		}
		if err := tp.Write(NewWriter(io.Discard), time.Unix(0, 0)); err == nil {
			t.Errorf("%s Write accepted", tp)
		}
	}
}

func TestStringBytesHugeLength(t *testing.T) {
	var v []byte
	err := StringBytes.Scan(NewReader(bytes.NewReader(append(binary.AppendUvarint(nil, 1<<62), 1, 2, 3))), &v)
	if err == nil {
		t.Fatal("length 2^62 accepted")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		// Any error is fine on 64-bit; the point is no panic and no huge allocation.
		t.Logf("length 2^62: %v", err)
	}
}

func TestStringBytesDeclaredLengthDoesNotPreallocate(t *testing.T) {
	const declared = 256 << 20
	in := append(binary.AppendUvarint(nil, declared), make([]byte, 16)...)
	var v []byte
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := StringBytes.Scan(NewReader(bytes.NewReader(in)), &v)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 16<<20 {
		t.Fatalf("allocated %d bytes for 16 bytes of input", grew)
	}
}

func TestStringBytesLargeValueRoundTrip(t *testing.T) {
	// Larger than the 1 MiB reader buffer, so the slow path runs.
	want := bytes.Repeat([]byte("rowbinary"), 300_000)
	var buf bytes.Buffer
	if err := StringBytes.Write(NewWriter(&buf), want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 0, 8) // reused destination with too little capacity
	if err := StringBytes.Scan(NewReader(bytes.NewReader(buf.Bytes())), &got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(want))
	}
}
