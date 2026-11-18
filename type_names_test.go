package rowbinary

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestDecodeStringTypeRejectsMalformedNames(t *testing.T) {
	for _, s := range []string{
		"",
		"   ",
		"Array(UInt8))",
		"Array(UInt8",
		"UInt8)",
		"Map(String, Array(UInt8)",
		"Variant(,)",
		"Tuple(UInt8, )",
		"Enum8('a = 1)",
	} {
		if tp, err := DecodeStringType(s); err == nil {
			t.Errorf("%q decoded as %s (% x)", s, tp, tp.Binary())
		}
	}
}

func TestDecodeStringTypeEmptyTuple(t *testing.T) {
	// ClickHouse 25.9: toTypeName(()) = Tuple(), binary encoding 1f 00.
	tp, err := DecodeStringType("Tuple()")
	if err != nil {
		t.Fatal(err)
	}
	if got := tp.Binary(); !bytes.Equal(got, []byte{0x1f, 0x00}) {
		t.Fatalf("Tuple() binary = % x, want 1f 00", got)
	}
	if tp.String() != "Tuple()" {
		t.Fatalf("Tuple() string = %s", tp)
	}
	btp, err := DecodeBinaryType(NewReader(bytes.NewReader([]byte{0x1f, 0x00})))
	if err != nil || !Eq(tp, btp) {
		t.Fatalf("binary Tuple() = %v, %v; want same type as text", btp, err)
	}
}

func TestDecodeStringTypeKeepsValidNames(t *testing.T) {
	for _, s := range []string{
		"Point",                               // geo types arrive as unknown simple names
		"MultiPolygon",                        //
		"Enum8('a)' = 1, 'b(' = 2)",           // brackets inside quotes
		"Enum8('it\\'s' = 1)",                 // escaped quote
		"Map(String, Array(Nullable(UInt8)))", // nesting
	} {
		if _, err := DecodeStringType(s); err != nil {
			t.Errorf("%q rejected: %v", s, err)
		}
	}
}

func TestFormatReaderReportsMissingOptionColumns(t *testing.T) {
	for _, format := range []Format{RowBinaryWithNames, RowBinaryWithNamesAndTypes} {
		var buf bytes.Buffer
		w := NewFormatWriter(&buf, format, C("value", UInt8))
		if err := Write(w, UInt8, 1); err != nil {
			t.Fatal(err)
		}
		var x uint8

		// Typo in the option column name.
		r := NewFormatReader(bytes.NewReader(buf.Bytes()), format, C("value", UInt8), C("valeu", UInt8))
		if err := Scan(r, UInt8, &x); err == nil {
			t.Errorf("%s: missing option column not reported", format)
		}

		// Exact column list still works.
		r = NewFormatReader(bytes.NewReader(buf.Bytes()), format, C("value", UInt8))
		if err := Scan(r, UInt8, &x); err != nil || x != 1 {
			t.Errorf("%s: matching columns: x=%d err=%v", format, x, err)
		}
	}
}

func TestDecodersReturnNotImplementedError(t *testing.T) {
	for _, s := range []string{"UInt128", "Int256", "FixedString"} {
		if _, err := DecodeStringType(s); !errors.Is(err, NotImplementedError) {
			t.Errorf("DecodeStringType(%q) = %v, want NotImplementedError", s, err)
		}
	}
	for _, b := range [][]byte{{0x05}, {0x30}, {0x22, 0x7f}, {0xfe}} {
		if _, err := DecodeBinaryType(NewReader(bytes.NewReader(b))); !errors.Is(err, NotImplementedError) {
			t.Errorf("DecodeBinaryType(% x) = %v, want NotImplementedError", b, err)
		}
	}
}

func TestVariantWriteMismatchNamesTypes(t *testing.T) {
	err := Variant(UInt8).Write(NewWriter(io.Discard), Value{Type: String, Value: "x"})
	var mismatch TypeMismatchError
	if !errors.As(err, &mismatch) || mismatch.ExpectedType != "Variant(UInt8)" || mismatch.ActualType != "String" {
		t.Fatalf("err = %#v", err)
	}
}
