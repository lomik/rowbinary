package rowbinary

import (
	"bytes"
	"testing"
)

func TestBinaryResultDoesNotAliasGlobals(t *testing.T) {
	b := Int8.Binary()
	orig := b[0]
	b[0] = 0x15 // a caller modifying the returned slice
	defer func() { b[0] = orig }()

	if BinaryTypeInt8[0] != orig {
		t.Fatalf("BinaryTypeInt8 changed to %#x through Int8.Binary()", BinaryTypeInt8[0])
	}
	// Other types built on the same tag are unaffected.
	tp, err := DecodeBinaryType(NewReader(bytes.NewReader([]byte{0x23, 0x07}))) // Nullable(Int8)
	if err != nil {
		t.Fatal(err)
	}
	if got := tp.Binary(); !bytes.Equal(got, []byte{0x23, 0x07}) {
		t.Fatalf("Nullable(Int8).Binary() = % x", got)
	}
}

func TestBinaryResultIsCallerOwned(t *testing.T) {
	for _, tp := range []Any{UInt8, String, DateTime64(3), Array(UInt8), Nullable(Int32)} {
		want := bytes.Clone(tp.Binary())
		b := tp.Binary()
		for i := range b {
			b[i] ^= 0xff
		}
		_ = append(b[:1], 0xee, 0xee, 0xee) // writes into any spare capacity
		if got := tp.Binary(); !bytes.Equal(got, want) {
			t.Errorf("%s.Binary() changed to % x after caller edits, want % x", tp, got, want)
		}
	}
}

func TestDynamicWriteDoesNotAllocateForBinary(t *testing.T) {
	v := Value{Type: UInt8, Value: uint8(1)}
	var buf bytes.Buffer
	wb := NewWriter(&buf)
	tp := DynamicDefault
	allocs := testing.AllocsPerRun(1000, func() {
		buf.Reset()
		_ = tp.Write(wb, v)
	})
	if allocs > 0 {
		t.Fatalf("Dynamic.Write allocates %.0f times per value", allocs)
	}
}

func TestConstructorsDoNotKeepCallerSlices(t *testing.T) {
	types := []Any{UInt8, String}
	tuple := TupleAny(types...)
	cols := []Column{C("a", UInt8), C("b", String)}
	named := TupleNamedAny(cols...)
	known := []Any{UInt8}
	dyn := Dynamic(0, known...).(*typeWrapper[Value]).PreType.(typeWrapperAny[Value]).BaseType.(typeDynamic)

	types[0] = Float64
	cols[0] = C("z", Float64)
	known[0] = Float64

	if tuple.String() != "Tuple(UInt8, String)" {
		t.Errorf("TupleAny changed after caller edit: %s", tuple)
	}
	var buf bytes.Buffer
	if err := tuple.Write(NewWriter(&buf), []any{uint8(1), "x"}); err != nil {
		t.Errorf("TupleAny write after caller edit: %v", err)
	}
	if err := named.Write(NewWriter(&buf), []any{uint8(1), "x"}); err != nil {
		t.Errorf("TupleNamedAny write after caller edit: %v", err)
	}
	if !Eq(dyn.knownTypes[0], UInt8) {
		t.Errorf("Dynamic known types changed after caller edit: %s", dyn.knownTypes[0])
	}
}
