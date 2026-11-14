package rowbinary

import (
	"bytes"
	"testing"
)

func TestUnknownTextTypeDoesNotDecodeRows(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := VarintWrite(w, 1); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"x", "UnknownAuditType"} {
		if err := String.Write(w, s); err != nil {
			t.Fatal(err)
		}
	}
	buf.WriteByte(42)
	r := NewFormatReader(&buf, RowBinaryWithNamesAndTypes)
	for i := 0; i < 3 && r.Next(); i++ {
		var value any
		if err := r.Scan(&value); err != nil {
			return // Unsupported type must fail, not consume zero bytes successfully.
		}
	}
	if r.Err() == nil {
		t.Fatal("unknown type decoded rows without consuming payload or reporting an error")
	}
}
