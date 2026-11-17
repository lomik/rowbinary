package rowbinary

import (
	"bytes"
	"testing"
)

// ClickHouse 25.9 reads any non-zero Nullable marker as NULL (no value
// follows) and any non-zero Bool byte as true.
func TestNullableMarkerNonZeroIsNull(t *testing.T) {
	for m := 1; m <= 255; m++ {
		// Marker followed by the next row's marker 0 and value 7.
		r := NewReader(bytes.NewReader([]byte{byte(m), 0x00, 0x07}))
		var p *uint8 = new(uint8)
		if err := Nullable(UInt8).Scan(r, &p); err != nil || p != nil {
			t.Fatalf("marker %d: %v, %v; want NULL", m, p, err)
		}
		// The stream stays aligned: the next value is read from the next byte.
		if err := Nullable(UInt8).Scan(r, &p); err != nil || p == nil || *p != 7 {
			t.Fatalf("marker %d: next value %v, %v; want 7", m, p, err)
		}

		var a *any
		if err := NullableAny(UInt8).Scan(NewReader(bytes.NewReader([]byte{byte(m)})), &a); err != nil || a != nil {
			t.Fatalf("NullableAny marker %d: %v, %v; want NULL", m, a, err)
		}
	}
	var p *uint8
	if err := Nullable(UInt8).Scan(NewReader(bytes.NewReader([]byte{0x00, 0x09})), &p); err != nil || p == nil || *p != 9 {
		t.Fatalf("marker 0: %v, %v; want 9", p, err)
	}
}

func TestBoolNonZeroIsTrue(t *testing.T) {
	for b := 0; b <= 255; b++ {
		var v bool
		if err := Bool.Scan(NewReader(bytes.NewReader([]byte{byte(b)})), &v); err != nil {
			t.Fatal(err)
		}
		if v != (b != 0) {
			t.Fatalf("byte %d read as %v", b, v)
		}
	}
	// Writing still produces only 0 and 1.
	for _, v := range []bool{false, true} {
		var buf bytes.Buffer
		if err := Bool.Write(NewWriter(&buf), v); err != nil {
			t.Fatal(err)
		}
		want := byte(0)
		if v {
			want = 1
		}
		if !bytes.Equal(buf.Bytes(), []byte{want}) {
			t.Fatalf("Write(%v) = % x", v, buf.Bytes())
		}
	}
}
