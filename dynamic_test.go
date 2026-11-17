package rowbinary

import (
	"bytes"
	"testing"
)

func TestDynamicNullRoundTrip(t *testing.T) {
	tp := DynamicDefault
	var value Value
	if err := tp.Scan(NewReader(bytes.NewReader([]byte{0})), &value); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tp.Write(NewWriter(&buf), value); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), []byte{0}) {
		t.Fatalf("wire = %x, want 00", buf.Bytes())
	}
}

func TestNothingWriteAnyNil(t *testing.T) {
	var buf bytes.Buffer
	if err := Nothing.WriteAny(NewWriter(&buf), nil); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatal("Nothing wrote bytes")
	}
}

func TestDynamicMaxTypesZero(t *testing.T) {
	// ClickHouse 25.9: Dynamic(max_types=0) is valid and encodes as 2b 00.
	d := Dynamic(0)
	if got := d.Binary(); !bytes.Equal(got, []byte{0x2b, 0x00}) {
		t.Fatalf("Dynamic(0).Binary() = % x, want 2b 00", got)
	}
	if d.String() != "Dynamic(max_types=0)" {
		t.Fatalf("Dynamic(0).String() = %s", d)
	}
	if Eq(d, DynamicDefault) {
		t.Fatal("Dynamic(0) equals the default Dynamic")
	}

	text, err := DecodeStringType("Dynamic(max_types=0)")
	if err != nil || !Eq(text, d) {
		t.Fatalf("text Dynamic(max_types=0) = %v, %v", text, err)
	}
	bin, err := DecodeBinaryType(NewReader(bytes.NewReader([]byte{0x2b, 0x00})))
	if err != nil || !Eq(bin, d) {
		t.Fatalf("binary 2b 00 = %v, %v", bin, err)
	}
}

func TestDynamicDefault(t *testing.T) {
	if got := DynamicDefault.Binary(); !bytes.Equal(got, []byte{0x2b, 0x20}) {
		t.Fatalf("DynamicDefault.Binary() = % x, want 2b 20", got)
	}
	if DynamicDefault.String() != "Dynamic" {
		t.Fatalf("DynamicDefault.String() = %s", DynamicDefault)
	}
	text, err := DecodeStringType("Dynamic")
	if err != nil || !Eq(text, DynamicDefault) || !Eq(text, Dynamic(DynamicDefaultMaxTypes)) {
		t.Fatalf("text Dynamic = %v, %v", text, err)
	}
}
