package rowbinary

import (
	"bytes"
	"testing"
)

func TestDynamicNullRoundTrip(t *testing.T) {
	tp := Dynamic(0)
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
