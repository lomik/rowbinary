package rowbinary

import (
	"bytes"
	"io"
	"testing"
)

func scanAnyBytes(t *testing.T, tp Any, data []byte) any {
	t.Helper()
	var v any
	if err := tp.ScanAny(NewReader(bytes.NewReader(data)), &v); err != nil {
		t.Fatalf("%s.ScanAny(% x): %v", tp, data, err)
	}
	return v
}

func TestNullableNullScansAsUntypedNil(t *testing.T) {
	for _, tp := range []Any{Nullable(String), NullableAny(String), Nullable(UInt8)} {
		if v := scanAnyBytes(t, tp, []byte{0x01}); v != nil {
			t.Errorf("%s NULL into *any = %#v (%T), want nil", tp, v, v)
		}
	}
	// Non-NULL values keep their pointer representation.
	if v, ok := scanAnyBytes(t, Nullable(String), []byte{0x00, 0x01, 'x'}).(*string); !ok || *v != "x" {
		t.Errorf("Nullable(String) non-NULL = %#v", v)
	}
	if v, ok := scanAnyBytes(t, NullableAny(String), []byte{0x00, 0x01, 'x'}).(*any); !ok || *v != "x" {
		t.Errorf("NullableAny(String) non-NULL = %#v", v)
	}
	// The typed path is unchanged: NULL is a nil *string.
	var p *string = new(string)
	if err := Nullable(String).Scan(NewReader(bytes.NewReader([]byte{0x01})), &p); err != nil || p != nil {
		t.Errorf("typed Scan NULL = %v, %v", p, err)
	}
}

func TestNullableNullInsideContainers(t *testing.T) {
	// Tuple(Nullable(String), UInt8) = (NULL, 7)
	tuple := scanAnyBytes(t, TupleAny(NullableAny(String), UInt8), []byte{0x01, 0x07}).([]any)
	if tuple[0] != nil || tuple[1] != uint8(7) {
		t.Errorf("tuple = %#v", tuple)
	}
	// Array(Nullable(UInt8)) = [NULL, 5]
	arr := scanAnyBytes(t, ArrayAny(NullableAny(UInt8)), []byte{0x02, 0x01, 0x00, 0x05}).([]any)
	if len(arr) != 2 || arr[0] != nil {
		t.Errorf("array = %#v", arr)
	}
	// Map(String, Nullable(UInt8)) = {'k': NULL}
	m := scanAnyBytes(t, MapAny(String, NullableAny(UInt8)), []byte{0x01, 0x01, 'k', 0x01}).(map[any]any)
	if v, ok := m["k"]; !ok || v != nil {
		t.Errorf("map = %#v", m)
	}
}

func TestFormatReaderScanNullIntoAny(t *testing.T) {
	for _, binary := range []bool{false, true} {
		var b bytes.Buffer
		w := NewFormatWriter(&b, RowBinaryWithNamesAndTypes, WithUseBinaryHeader(binary), C("v", Nullable(String)))
		if err := Write(w, Nullable(String), nil); err != nil {
			t.Fatal(err)
		}
		r := NewFormatReader(&b, RowBinaryWithNamesAndTypes, WithUseBinaryHeader(binary))
		var v any = "sentinel"
		if !r.Next() || r.Scan(&v) != nil || v != nil {
			t.Errorf("binary header %v: NULL scanned as %#v (%T), err %v", binary, v, v, r.Err())
		}
	}
}

func TestNullableWriteAnyNil(t *testing.T) {
	for _, tp := range []Any{Nullable(String), NullableAny(String)} {
		var buf bytes.Buffer
		if err := tp.WriteAny(NewWriter(&buf), nil); err != nil {
			t.Errorf("%s.WriteAny(nil): %v", tp, err)
			continue
		}
		if !bytes.Equal(buf.Bytes(), []byte{0x01}) {
			t.Errorf("%s.WriteAny(nil) = % x, want 01", tp, buf.Bytes())
		}
		// Round trip: what ScanAny returns, WriteAny accepts.
		v := scanAnyBytes(t, tp, buf.Bytes())
		if err := tp.WriteAny(NewWriter(io.Discard), v); err != nil {
			t.Errorf("%s: WriteAny of scanned NULL: %v", tp, err)
		}
	}
	// Non-nullable types still reject nil.
	if err := String.WriteAny(NewWriter(io.Discard), nil); err == nil {
		t.Error("String.WriteAny(nil) accepted")
	}
}
