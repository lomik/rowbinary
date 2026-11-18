package rowbinary

import (
	"bytes"
	"io"
	"testing"

	"github.com/shopspring/decimal"
)

func TestInvalidIsDistinctFromNothing(t *testing.T) {
	dec := Decimal(80, 1) // precision above 76: Invalid
	if Eq(dec, Nothing) {
		t.Fatal("Eq(Decimal(80, 1), Nothing) = true")
	}
	if b := dec.Binary(); len(b) == 0 || b[0] != 0xff {
		t.Fatalf("Invalid.Binary() = % x, want 0xff marker", b)
	}
	if Eq(Invalid[int]("a"), Invalid[int]("b")) {
		t.Fatal("Invalid types with different messages share an ID")
	}
	if err := dec.Write(NewWriter(io.Discard), decimal.NewFromInt(1)); err == nil {
		t.Fatal("Invalid.Write returned nil")
	}
	// A header written from an Invalid type must not decode as a real type.
	if tp, err := DecodeBinaryType(NewReader(bytes.NewReader(dec.Binary()))); err == nil {
		t.Fatalf("Invalid encoding decoded as %s", tp)
	}
}

func TestVariantDoesNotWriteInvalidAsNull(t *testing.T) {
	var buf bytes.Buffer
	err := Variant(UInt8).Write(NewWriter(&buf), Value{Type: Decimal(80, 1), Value: nil})
	if err == nil {
		t.Fatalf("Variant wrote an Invalid-typed value as % x", buf.Bytes())
	}

	// A real NULL still encodes as 0xff.
	buf.Reset()
	if err := Variant(UInt8).Write(NewWriter(&buf), Value{Type: Nothing, Value: nil}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), []byte{0xff}) {
		t.Fatalf("Variant NULL = % x, want ff", buf.Bytes())
	}
}
