package rowbinary

import (
	"bytes"
	"testing"
)

func TestFormatReaderHugeColumnCount(t *testing.T) {
	for _, format := range []Format{RowBinaryWithNames, RowBinaryWithNamesAndTypes} {
		t.Run(format.String(), func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("column count caused panic: %v", p)
				}
			}()
			r := NewFormatReader(bytes.NewReader(VarintEncode(^uint64(0))), format)
			if r.Next() || r.Err() == nil {
				t.Fatal("expected error for missing declared column names")
			}
		})
	}
}
