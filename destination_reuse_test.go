package rowbinary

import (
	"bytes"
	"testing"
)

// encode writes values with tp into one stream.
func encode[T any](t *testing.T, tp Type[T], values ...T) Reader {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, v := range values {
		if err := tp.Write(w, v); err != nil {
			t.Fatal(err)
		}
	}
	return NewReader(bytes.NewReader(buf.Bytes()))
}

func TestReusedDestinationsHoldNoStaleData(t *testing.T) {
	// Slices shrink to the new length.
	{
		r := encode(t, Array(String), []string{"a", "b", "c"}, []string{"x"})
		dst := make([]string, 0, 8)
		_ = Array(String).Scan(r, &dst)
		if err := Array(String).Scan(r, &dst); err != nil || len(dst) != 1 || dst[0] != "x" {
			t.Errorf("Array(String) reuse: %q, %v", dst, err)
		}
	}
	{
		r := encode(t, FixedString(2), []byte("ab"))
		dst := []byte("longer than two")
		if err := FixedString(2).Scan(r, &dst); err != nil || string(dst) != "ab" {
			t.Errorf("FixedString reuse: %q, %v", dst, err)
		}
	}
	{
		r := encode(t, TupleAny(UInt8, String), []any{uint8(1), "a"})
		dst := []any{"stale", "stale", "stale"}
		if err := TupleAny(UInt8, String).Scan(r, &dst); err != nil || len(dst) != 2 || dst[0] != uint8(1) || dst[1] != "a" {
			t.Errorf("TupleAny reuse: %v, %v", dst, err)
		}
	}
	// Maps lose keys from the previous value.
	{
		r := encode(t, Map(String, UInt8), map[string]uint8{"a": 1, "b": 2}, map[string]uint8{"c": 3})
		var dst map[string]uint8
		_ = Map(String, UInt8).Scan(r, &dst)
		if err := Map(String, UInt8).Scan(r, &dst); err != nil || len(dst) != 1 || dst["c"] != 3 {
			t.Errorf("Map reuse: %v, %v", dst, err)
		}
	}
	{
		first := NewKV[string, uint8]().Append("a", 1).Append("b", 2)
		second := NewKV[string, uint8]().Append("c", 3)
		r := encode(t, MapKV(String, UInt8), first, second)
		var dst *KV[string, uint8]
		_ = MapKV(String, UInt8).Scan(r, &dst)
		if err := MapKV(String, UInt8).Scan(r, &dst); err != nil || dst.Len() != 1 {
			t.Errorf("MapKV reuse: len %d, %v", dst.Len(), err)
		}
	}
	// A NULL clears a reused pointer.
	{
		one := int64(1)
		r := encode(t, Nullable(Int64), &one, nil)
		var dst *int64
		_ = Nullable(Int64).Scan(r, &dst)
		if err := Nullable(Int64).Scan(r, &dst); err != nil || dst != nil {
			t.Errorf("Nullable reuse after NULL: %v, %v", dst, err)
		}
	}
}

// Values already returned must not change when the next value is read: they
// must not alias the reader's buffer or a value shared with later results.
func TestScannedValuesDoNotAliasLaterReads(t *testing.T) {
	{
		r := encode(t, StringBytes, []byte("hello"), []byte("world"))
		var first, second []byte
		_ = StringBytes.Scan(r, &first)
		_ = StringBytes.Scan(r, &second)
		if string(first) != "hello" || string(second) != "world" {
			t.Errorf("StringBytes: %q, %q", first, second)
		}
	}
	{
		r := encode(t, FixedString(3), []byte("abc"), []byte("xyz"))
		var first, second []byte
		_ = FixedString(3).Scan(r, &first)
		_ = FixedString(3).Scan(r, &second)
		if string(first) != "abc" || string(second) != "xyz" {
			t.Errorf("FixedString: %q, %q", first, second)
		}
	}
	{
		one, two := int64(1), int64(2)
		r := encode(t, Nullable(Int64), &one, &two)
		var dst *int64
		_ = Nullable(Int64).Scan(r, &dst)
		first := dst
		_ = Nullable(Int64).Scan(r, &dst)
		if *first != 1 || *dst != 2 {
			t.Errorf("Nullable: first %d, second %d", *first, *dst)
		}
	}
	{
		r := encode(t, ArrayAny(StringBytes), []any{[]byte("ab")}, []any{[]byte("cd")})
		var first, second []any
		_ = ArrayAny(StringBytes).Scan(r, &first)
		_ = ArrayAny(StringBytes).Scan(r, &second)
		if string(first[0].([]byte)) != "ab" || string(second[0].([]byte)) != "cd" {
			t.Errorf("ArrayAny(StringBytes): %q, %q", first, second)
		}
	}
	// Through FormatReader, row by row into fresh variables.
	{
		var buf bytes.Buffer
		w := NewFormatWriter(&buf, RowBinaryWithNamesAndTypes, C("s", StringBytes))
		_ = Write(w, StringBytes, []byte("row1"))
		_ = Write(w, StringBytes, []byte("row2"))
		fr := NewFormatReader(&buf, RowBinaryWithNamesAndTypes)
		var rows [][]byte
		for fr.Next() {
			var s []byte
			if err := Scan(fr, StringBytes, &s); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, s)
		}
		if len(rows) != 2 || string(rows[0]) != "row1" || string(rows[1]) != "row2" {
			t.Errorf("FormatReader rows: %q", rows)
		}
	}
}
