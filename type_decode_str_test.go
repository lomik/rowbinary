package rowbinary

import (
	"bytes"
	"strings"
	"testing"
)

func TestDecodeStringTypeLeadingWhitespace(t *testing.T) {
	for _, spaces := range []int{1, 32} {
		t.Run(strings.Repeat("_", spaces), func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("leading whitespace caused panic: %v", p)
				}
			}()
			tp, err := DecodeStringType(strings.Repeat(" ", spaces) + "Array(UInt8) \t")
			if err != nil {
				t.Fatal(err)
			}
			if !Eq(tp, Array(UInt8)) {
				t.Fatalf("got %s, want Array(UInt8)", tp.String())
			}
		})
	}
}

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
