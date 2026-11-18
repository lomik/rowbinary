package rowbinary

import (
	"bytes"
	"testing"
)

// Fuzz targets for input that comes from a server or a user: type names,
// binary type headers and whole RowBinaryWithNamesAndTypes streams. Run one
// with, for example:
//
//	go test -run '^$' -fuzz '^FuzzDecodeBinaryType$' -fuzztime 30s .

var fuzzSeedTypes = []Any{
	UInt8, Int64, Float64, String, Bool, UUID, IPv4, IPv6, Date, Date32,
	DateTime, DateTimeTZ("UTC"), DateTime64(3), DateTime64TZ(9, "Asia/Shanghai"),
	FixedString(4), Decimal(9, 2), Decimal(38, 4), Decimal(76, 10),
	Enum8(map[string]int8{"a, b": 1, "it's": 2}), Enum16(map[string]int16{"x": -300}),
	ArrayAny(NullableAny(String)), MapAny(String, ArrayAny(UInt8)),
	TupleAny(UInt8, String), TupleNamedAny(C("a", UInt8), C("b", String)),
	LowCardinalityAny(String), Variant(UInt8, String), Dynamic(0), DynamicDefault,
	IntervalYear, TupleAny(), Nothing,
}

func FuzzDecodeStringType(f *testing.F) {
	for _, tp := range fuzzSeedTypes {
		f.Add(tp.String())
	}
	for _, s := range []string{"", "Array(", "Enum8('a' = 1, 'b' = 1)", "Decimal(77, 2)", "Map(String)", "Tuple(a UInt8, b)"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		tp, err := DecodeStringType(s)
		if err != nil {
			return
		}
		// A name that parses must parse again from the type's own String().
		again, err := DecodeStringType(tp.String())
		if err != nil {
			t.Fatalf("%q decoded as %s, which does not decode: %v", s, tp, err)
		}
		if !Eq(tp, again) {
			t.Fatalf("%q decoded as %s (% x); re-decoded as %s (% x)", s, tp, tp.Binary(), again, again.Binary())
		}
	})
}

func FuzzDecodeBinaryType(f *testing.F) {
	for _, tp := range fuzzSeedTypes {
		f.Add(tp.Binary())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		tp, err := DecodeBinaryType(NewReader(bytes.NewReader(data)))
		if err != nil {
			return
		}
		// A header that decodes must round-trip through Binary().
		again, err := DecodeBinaryType(NewReader(bytes.NewReader(tp.Binary())))
		if err != nil {
			t.Fatalf("% x decoded as %s, whose encoding % x does not decode: %v", data, tp, tp.Binary(), err)
		}
		if !Eq(tp, again) {
			t.Fatalf("% x decoded as %s; re-decoded as %s", data, tp, again)
		}
	})
}

func FuzzFormatReader(f *testing.F) {
	for _, binaryHeader := range []bool{false, true} {
		for _, tp := range fuzzSeedTypes {
			if tp.String() == "Nothing" || tp.String() == "Tuple()" {
				continue
			}
			var buf bytes.Buffer
			w := NewFormatWriter(&buf, RowBinaryWithNamesAndTypes, WithUseBinaryHeader(binaryHeader), C("v", tp))
			if w.WriteHeader() == nil {
				f.Add(binaryHeader, append(buf.Bytes(), 0, 0, 0, 0, 0, 0, 0, 0))
			}
		}
	}
	f.Fuzz(func(t *testing.T, binaryHeader bool, data []byte) {
		r := NewFormatReader(bytes.NewReader(data), RowBinaryWithNamesAndTypes, WithUseBinaryHeader(binaryHeader))
		cols, err := r.Columns()
		if err != nil {
			return
		}
		n := len(cols.cols)
		// Every value consumes at least one byte or the stream ends, so the
		// number of rows is bounded by the input; cap it anyway.
		for rows := 0; rows <= len(data) && r.Next(); rows++ {
			dest := make([]any, n)
			ptrs := make([]any, n)
			for i := range dest {
				ptrs[i] = &dest[i]
			}
			if r.Scan(ptrs...) != nil {
				return
			}
		}
	})
}
