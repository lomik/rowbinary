package rowbinary

import (
	"bytes"
	"io"
	"testing"
)

func TestZeroValueWriteReturnsError(t *testing.T) {
	for name, tp := range map[string]Type[Value]{
		"Variant": Variant(UInt8),
		"Dynamic": Dynamic(0),
	} {
		if err := tp.Write(NewWriter(io.Discard), Value{}); err == nil {
			t.Errorf("%s.Write(Value{}) returned nil", name)
		}
	}
	if s := (Value{}).String(); s == "" {
		t.Error("Value{}.String() is empty")
	}
}

func TestMapKVWriteNilIsEmptyMap(t *testing.T) {
	tp := MapKV(String, UInt8)
	var got, want bytes.Buffer
	if err := tp.Write(NewWriter(&got), nil); err != nil {
		t.Fatal(err)
	}
	if err := tp.Write(NewWriter(&want), NewKV[string, uint8]()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatalf("nil *KV wrote % x, empty KV writes % x", got.Bytes(), want.Bytes())
	}
}

func TestMapAnyUnhashableKeyReturnsError(t *testing.T) {
	var m map[any]any
	// Map(String, UInt8) with one entry {"a": 5}, read with a []byte key codec.
	err := MapAny(StringBytes, UInt8).Scan(NewReader(bytes.NewReader([]byte{1, 1, 'a', 5})), &m)
	if err == nil {
		t.Fatal("MapAny with []byte keys returned nil")
	}

	// Hashable keys keep working.
	if err := MapAny(String, UInt8).Scan(NewReader(bytes.NewReader([]byte{1, 1, 'a', 5})), &m); err != nil {
		t.Fatal(err)
	}
	if m["a"] != uint8(5) {
		t.Fatalf("m = %v", m)
	}
}
