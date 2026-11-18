package rowbinary

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestEnumDuplicateValuesAreInvalid(t *testing.T) {
	for _, tp := range []Type[string]{
		Enum8(map[string]int8{"a": 1, "b": 1}),
		Enum16(map[string]int16{"a": 1, "b": 1, "c": 2}),
	} {
		err := tp.Write(NewWriter(io.Discard), "a")
		if err == nil || !strings.Contains(err.Error(), "used by both 'a' and 'b'") {
			t.Errorf("%s: Write error = %v", tp, err)
		}
		var s string
		if err := tp.Scan(NewReader(bytes.NewReader([]byte{1, 0})), &s); err == nil {
			t.Errorf("%s: Scan accepted", tp)
		}
	}
	// Distinct values are unaffected.
	if tp := Enum8(map[string]int8{"a": 1, "b": 2}); tp.String() != "Enum8('a' = 1, 'b' = 2)" {
		t.Errorf("valid enum = %s", tp)
	}
}

func TestVariantConstructorMatchesClickHouse(t *testing.T) {
	// ClickHouse 25.9: CAST(1, 'Variant(UInt8, UInt8)') has type Variant(UInt8).
	v := Variant(UInt8, UInt8, String, UInt8)
	if !Eq(v, Variant(String, UInt8)) || v.String() != "Variant(String, UInt8)" {
		t.Errorf("duplicates not merged: %s % x", v, v.Binary())
	}

	types := func(n int) []Any {
		out := make([]Any, n)
		for i := range out {
			out[i] = FixedString(i + 1)
		}
		return out
	}
	if err := Variant(types(255)...).Write(NewWriter(io.Discard), Value{Type: FixedString(1), Value: []byte{1}}); err != nil {
		t.Errorf("255 types: %v", err)
	}
	if err := Variant(types(256)...).Write(NewWriter(io.Discard), Value{Type: FixedString(1), Value: []byte{1}}); err == nil {
		t.Error("256 types accepted")
	}
}

func TestFixedStringSizeIsValidated(t *testing.T) {
	for _, n := range []int{-1, 0, maxFixedStringSize + 1} {
		if err := FixedString(n).Write(NewWriter(io.Discard), make([]byte, max(n, 0))); err == nil {
			t.Errorf("FixedString(%d) accepted", n)
		}
	}
	if err := FixedString(1).Write(NewWriter(io.Discard), []byte{7}); err != nil {
		t.Errorf("FixedString(1): %v", err)
	}
}

func TestDecodersRejectEnumDuplicatesAndLargeVariants(t *testing.T) {
	for _, s := range []string{
		"Enum8('a' = 1, 'b' = 1)",
		"Enum8('a' = 1, 'a' = 2)",
		"Enum16('a' = 1, 'b' = 1)",
	} {
		if tp, err := DecodeStringType(s); err == nil {
			t.Errorf("%s decoded as %s", s, tp)
		}
	}
	enum8 := func(entries ...any) []byte {
		b := append([]byte{0x17}, binary.AppendUvarint(nil, uint64(len(entries)/2))...)
		for i := 0; i < len(entries); i += 2 {
			b = append(b, StringEncode(entries[i].(string))...)
			b = append(b, byte(entries[i+1].(int)))
		}
		return b
	}
	for name, h := range map[string][]byte{
		"duplicate value": enum8("a", 1, "b", 1),
		"duplicate name":  enum8("a", 1, "a", 2),
		"256 variants":    append([]byte{0x2a}, binary.AppendUvarint(nil, 256)...),
	} {
		if tp, err := DecodeBinaryType(NewReader(bytes.NewReader(h))); err == nil {
			t.Errorf("%s decoded as %s", name, tp)
		}
	}
	args := make([]string, 256)
	for i := range args {
		args[i] = fmt.Sprintf("FixedString(%d)", i+1)
	}
	if _, err := DecodeStringType("Variant(" + strings.Join(args, ", ") + ")"); err == nil {
		t.Error("text Variant with 256 types accepted")
	}
	tp, err := DecodeStringType("Variant(UInt8, UInt8)")
	if err != nil || !Eq(tp, Variant(UInt8)) {
		t.Errorf("Variant(UInt8, UInt8) = %v, %v; want Variant(UInt8)", tp, err)
	}
}

func TestLargeEnum16HeaderDecodes(t *testing.T) {
	// 65536 distinct entries: duplicate checks must stay linear.
	b := append([]byte{0x18}, binary.AppendUvarint(nil, 65536)...)
	for v := -32768; v <= 32767; v++ {
		b = append(b, StringEncode(fmt.Sprint(v))...)
		b = binary.LittleEndian.AppendUint16(b, uint16(int16(v)))
	}
	if _, err := DecodeBinaryType(NewReader(bytes.NewReader(b))); err != nil {
		t.Fatal(err)
	}
}
