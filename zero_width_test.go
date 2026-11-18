package rowbinary

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

func TestIsZeroWidthType(t *testing.T) {
	for _, tp := range []Any{Nothing, TupleAny(), TupleAny(Nothing, TupleAny()), TupleNamedAny(C("a", Nothing)), LowCardinalityAny(Nothing)} {
		if !isZeroWidthType(tp) {
			t.Errorf("%s: want zero width", tp)
		}
	}
	for _, tp := range []Any{UInt8, String, Nullable(Nothing), ArrayAny(Nothing), TupleAny(UInt8), TupleAny(Nothing, UInt8), MapAny(Nothing, Nothing), DynamicDefault} {
		if isZeroWidthType(tp) {
			t.Errorf("%s: want non-zero width", tp)
		}
	}
}

// binaryHeader returns a RowBinaryWithNamesAndTypes binary header with one column.
func binaryHeader(t *testing.T, tp Any) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewFormatWriter(&buf, RowBinaryWithNamesAndTypes, WithUseBinaryHeader(true), C("v", tp))
	if err := w.WriteHeader(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func uvarint(n uint64) []byte { return binary.AppendUvarint(nil, n) }

func scanRows(data []byte) (rows int, err error) {
	r := NewFormatReader(bytes.NewReader(data), RowBinaryWithNamesAndTypes, WithUseBinaryHeader(true))
	for r.Next() {
		var v any
		if err := r.Scan(&v); err != nil {
			return rows, err
		}
		rows++
	}
	return rows, r.Err()
}

func TestZeroWidthArrayLimitInFormatReader(t *testing.T) {
	start := time.Now()
	// The fuzzer's case: a few bytes declaring 2^40 elements of Array(Nothing).
	if _, err := scanRows(append(binaryHeader(t, ArrayAny(Nothing)), uvarint(1<<40)...)); err == nil || !strings.Contains(err.Error(), "zero width") {
		t.Errorf("huge Array(Nothing): err = %v", err)
	}
	// Nesting shares one budget per row: two inner arrays of 600k exceed it.
	nested := append(binaryHeader(t, ArrayAny(ArrayAny(TupleAny()))), uvarint(2)...)
	nested = append(append(nested, uvarint(600_000)...), uvarint(600_000)...)
	if _, err := scanRows(nested); err == nil {
		t.Error("nested zero-width arrays over the row budget accepted")
	}
	// The budget is per row: three rows of 600k each are fine.
	rowsData := binaryHeader(t, ArrayAny(TupleAny()))
	for range 3 {
		rowsData = append(rowsData, uvarint(600_000)...)
	}
	if rows, err := scanRows(rowsData); err != nil || rows != 3 {
		t.Errorf("rows under the budget: %d rows, err %v", rows, err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("took %s", d)
	}
}

func TestZeroWidthLimitWithoutFormatReader(t *testing.T) {
	var v []any
	if err := ArrayAny(Nothing).Scan(NewReader(bytes.NewReader(uvarint(maxZeroWidthElements+1))), &v); err == nil {
		t.Error("Array(Nothing) over the limit accepted")
	}
	if err := ArrayAny(Nothing).Scan(NewReader(bytes.NewReader(uvarint(1000))), &v); err != nil || len(v) != 1000 {
		t.Errorf("1000 elements: len %d, err %v", len(v), err)
	}
	var m map[any]any
	if err := MapAny(Nothing, Nothing).Scan(NewReader(bytes.NewReader(uvarint(1<<40))), &m); err == nil {
		t.Error("Map(Nothing, Nothing) over the limit accepted")
	}
	var typed [][]any
	if err := Array(TupleAny()).Scan(NewReader(bytes.NewReader(uvarint(1<<40))), &typed); err == nil {
		t.Error("typed Array(Tuple()) over the limit accepted")
	}
}

func TestSmallZeroWidthArraysRoundTrip(t *testing.T) {
	tp := ArrayAny(TupleAny())
	var buf bytes.Buffer
	if err := tp.Write(NewWriter(&buf), []any{[]any{}, []any{}, []any{}}); err != nil {
		t.Fatal(err)
	}
	var v []any
	if err := tp.Scan(NewReader(bytes.NewReader(buf.Bytes())), &v); err != nil || len(v) != 3 {
		t.Fatalf("len %d, err %v", len(v), err)
	}
}
